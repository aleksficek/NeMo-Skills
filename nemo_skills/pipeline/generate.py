# Copyright (c) 2024, NVIDIA CORPORATION.  All rights reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
import copy
import logging
import os
import shlex
import subprocess
from collections import defaultdict
from enum import Enum
from typing import List

import typer

from nemo_skills.inference.generate import GenerationTask
from nemo_skills.pipeline.app import app, typer_unpacker
from nemo_skills.pipeline.utils import (
    SupportedServers,
    add_task,
    check_mounts,
    get_cluster_config,
    get_exp,
    get_free_port,
    get_generation_command,
    get_reward_server_command,
    get_server_command,
    get_tunnel,
    get_unmounted_path,
    resolve_mount_paths,
    run_exp,
    wrap_cmd,
)
from nemo_skills.utils import compute_chunk_ids, get_chunked_filename, get_logger_name, setup_logging, str_ids_to_list

LOG = logging.getLogger(get_logger_name(__file__))


def get_chunked_rs_filename(
    output_dir: str,
    random_seed: int = None,
    chunk_id: int = None,
    output_prefix: str = "output",
) -> str:
    """
    Return a path of the form:
      {output_dir}/{output_prefix}[-rsSEED][-chunkK].jsonl
    If `output_prefix` is None, fallback to 'output' in place of {output_prefix}.
    """
    if random_seed is not None:
        base_filename = f"{output_prefix}-rs{random_seed}.jsonl"
    else:
        base_filename = f"{output_prefix}.jsonl"

    # If chunking is enabled, add the chunk suffix
    if chunk_id is not None:
        base_filename = get_chunked_filename(chunk_id, base_filename)
    return os.path.join(output_dir, base_filename)


def get_expected_done_files(output_dir, random_seeds, chunk_ids, output_prefix="output"):
    """
    Returns a mapping of (seed, chunk_id) to expected .done file paths
    """
    file_map = {}
    for seed in random_seeds:
        for chunk_id in chunk_ids:
            output_file = get_chunked_rs_filename(
                output_dir, random_seed=seed, chunk_id=chunk_id, output_prefix=output_prefix
            )
            file_map[(seed, chunk_id)] = f"{output_file}.done"
    return file_map


def get_remaining_jobs(cluster_config, output_dir, random_seeds, chunk_ids, rerun_done, output_prefix="output"):
    """
    Determines which jobs still need to be run based on missing .done files.
    Returns a mapping from random_seed to list of chunk_ids that need processing.
    """
    if rerun_done:
        return {seed: copy.deepcopy(chunk_ids) for seed in random_seeds}

    status_dir = get_unmounted_path(cluster_config, output_dir)
    expected_files = get_expected_done_files(output_dir, random_seeds, chunk_ids, output_prefix=output_prefix)

    check_commands = []
    for (seed, chunk_id), filepath in expected_files.items():
        unmounted_path = filepath.replace(output_dir, status_dir)
        # Create identifiers that can be parsed from output
        seed_str = "NONE" if seed is None else str(seed)
        chunk_str = "NONE" if chunk_id is None else str(chunk_id)
        check_commands.append(f'if [ ! -f "{unmounted_path}" ]; then echo "MISSING:{seed_str}:{chunk_str}"; fi')

    # If random_seeds has more than N elements, split commands into groups of N
    request_size = 16
    if len(random_seeds) > request_size:
        outputs = []
        for i in range(0, len(check_commands), request_size):
            group = check_commands[i : i + request_size]
            command = f"bash -c '{'; '.join(group)}'"
            if cluster_config['executor'] == 'slurm':
                out = get_tunnel(cluster_config).run(command).stdout.strip()
            else:
                out = subprocess.run(command, shell=True, check=True, stdout=subprocess.PIPE).stdout.decode("utf-8")
            outputs.append(out)
        output = "\n".join(outputs).strip()
    else:
        command = f"bash -c '{'; '.join(check_commands)}'"
        if cluster_config['executor'] == 'slurm':
            output = get_tunnel(cluster_config).run(command).stdout.strip()
        else:
            output = subprocess.run(command, shell=True, check=True, stdout=subprocess.PIPE).stdout.decode("utf-8")

    # Parse results into a mapping of missing jobs
    missing_jobs = defaultdict(list)
    for line in output.splitlines():
        if line.startswith("MISSING:"):
            _, seed_str, chunk_str = line.split(":")
            seed = None if seed_str == "NONE" else int(seed_str)
            chunk = None if chunk_str == "NONE" else int(chunk_str)
            missing_jobs[seed].append(chunk)

    done_jobs = defaultdict(list)
    for seed, chunk_id in expected_files.keys():
        if chunk_id not in missing_jobs[seed]:
            done_jobs[seed].append(chunk_id)

    done_jobs_str = ", ".join(
        [
            (
                f"{seed}"
                if not any(chunk is not None for chunk in chunks)
                else f"{seed} (chunks: {', '.join(str(chunk) for chunk in chunks if chunk is not None)})"
            )
            for seed, chunks in done_jobs.items()
            if chunks
        ]
    )
    missing_jobs_str = ", ".join(
        [
            (
                f"{seed}"
                if not any(chunk is not None for chunk in chunks)
                else f"{seed} (chunks: {', '.join(str(chunk) for chunk in chunks if chunk is not None)})"
            )
            for seed, chunks in missing_jobs.items()
            if chunks
        ]
    )

    if missing_jobs_str:
        # only printing this if there are some missing and some done
        if done_jobs_str:
            LOG.warning(
                "The following jobs are incomplete and will be launched: seeds %s",
                missing_jobs_str,
            )
            LOG.warning(
                "The following jobs are completed and will be skipped (to override set --rerun_done): seeds %s",
                done_jobs_str,
            )
    else:
        LOG.warning("All jobs are completed. No jobs will be launched (to override set --rerun_done).")

    return missing_jobs


def get_cmd(
    output_dir,
    extra_arguments,
    random_seed=None,
    eval_args=None,
    chunk_id=None,
    num_chunks=None,
    postprocess_cmd=None,
    script: str = 'nemo_skills.inference.generate',
    output_prefix: str = "output",
):
    """
    Construct the generation command for language model inference.

    If chunk_id is provided, chunking logic is used.
    If output_prefix is provided, it replaces the default 'output*.jsonl' filenames
    with a base name (plus `-rsSEED` or chunk info as needed).
    """
    # First get the unchunked filename for the output file
    output_file = get_chunked_rs_filename(
        output_dir=output_dir,
        random_seed=random_seed,
        output_prefix=output_prefix,
    )
    cmd = f"python -m {script} ++skip_filled=True ++output_file={output_file} "
    job_end_cmd = ""

    if random_seed is not None:
        cmd += (
            f"    ++inference.random_seed={random_seed} "
            f"    ++inference.temperature=1.0 "
            f"    ++inference.top_k=0 "
            f"    ++inference.top_p=0.95 "
        )

    if chunk_id is not None:
        cmd += f" ++num_chunks={num_chunks} ++chunk_id={chunk_id} "
        output_file = get_chunked_rs_filename(
            output_dir, random_seed=random_seed, chunk_id=chunk_id, output_prefix=output_prefix
        )
        donefiles = []
        # we are always waiting for all chunks in num_chunks, no matter chunk_ids in
        # the current run (as we don't want to merge partial jobs)
        for cur_chunk_id in range(num_chunks):
            donefile = f"{get_chunked_rs_filename(output_dir=output_dir, random_seed=random_seed, chunk_id=cur_chunk_id, output_prefix=output_prefix)}.done"
            donefiles.append(donefile)

        if job_end_cmd:
            job_end_cmd += f" && touch {donefiles[chunk_id]} "
        else:
            job_end_cmd = f"touch {donefiles[chunk_id]} "

        # getting file name as if there is no chunking since that's where we want to merge
        merged_output_file = get_chunked_rs_filename(
            output_dir=output_dir, random_seed=random_seed, output_prefix=output_prefix
        )
        merge_cmd = (
            f"python -m nemo_skills.inference.merge_chunks {merged_output_file} "
            f"{' '.join([f[:-5] for f in donefiles])}"
        )
        if postprocess_cmd:
            postprocess_cmd = shlex.quote(postprocess_cmd)
            merge_cmd = f"{merge_cmd} -- {postprocess_cmd}"
        postprocess_cmd = f"{job_end_cmd} && {merge_cmd}"

    else:  # only writing a single status file
        if job_end_cmd:
            job_end_cmd += f" && touch {output_file}.done "
        else:
            job_end_cmd = f"touch {output_file}.done "

        if postprocess_cmd:
            postprocess_cmd = f"{job_end_cmd} && {postprocess_cmd}"
        else:
            postprocess_cmd = job_end_cmd

    cmd += f" {extra_arguments} "

    if eval_args:
        cmd += (
            f" && python -m nemo_skills.evaluation.evaluate_results "
            f"    ++input_files={output_file} "
            f"    {eval_args} "
        )

    return cmd, postprocess_cmd


# TODO: support chunking for reward model and math judge


def get_rm_cmd(
    output_dir,
    extra_arguments,
    random_seed=None,
    eval_args=None,
    chunk_id=None,
    num_chunks=None,
    postprocess_cmd=None,
    script: str = 'nemo_skills.inference.reward_model',
    output_prefix: str = "output",
):
    if eval_args is not None:
        raise ValueError("Cannot specify eval_args for reward model")

    cmd = (
        f"python -m {script} "
        f"    ++skip_filled=True "
        f"    ++output_dir={output_dir} "
        f"    ++random_seed={random_seed} "
    )
    cmd += f" {extra_arguments} "
    print(cmd)
    return cmd, postprocess_cmd


def get_math_judge_cmd(
    output_dir,
    extra_arguments,
    random_seed=None,
    eval_args=None,
    chunk_id=None,
    num_chunks=None,
    postprocess_cmd=None,
    script: str = 'nemo_skills.inference.llm_math_judge',
    output_prefix: str = "output",
):
    if eval_args is not None:
        raise ValueError("Cannot specify eval_args for math judge")
    cmd = (
        f"python -m {script} "
        f"    ++skip_filled=True "
        f"    ++output_dir={output_dir} "
        f"    ++random_seed={random_seed} "
    )
    cmd += f" {extra_arguments} "
    return cmd, postprocess_cmd


def get_genselect_cmd(
    output_dir,
    extra_arguments,
    random_seed=None,
    eval_args=None,
    chunk_id=None,
    num_chunks=None,
    postprocess_cmd=None,
    script: str = 'nemo_skills.inference.genselect',
    output_prefix: str = "output",
):
    if eval_args is not None:
        raise ValueError("Cannot specify eval_args for genselect")
    cmd = (
        f"python -m {script} "
        f"    ++skip_filled=True "
        f"    ++input_dir={output_dir}/comparison_instances "
        f"    ++output_dir={output_dir} "
        f"    ++inference.random_seed={random_seed} "
        f"    ++inference.temperature=0.7 "
        f"    ++inference.tokens_to_generate=2048 "
        f"    ++inference.top_k=0 "
        f"    ++inference.top_p=0.95 "
    )
    cmd += f" {extra_arguments} "
    return cmd, postprocess_cmd


class GenerationType(str, Enum):
    generate = "generate"
    reward = "reward"
    math_judge = "math_judge"
    genselect = "genselect"


server_command_factories = {
    GenerationType.generate: get_server_command,
    GenerationType.reward: get_reward_server_command,
    GenerationType.math_judge: get_server_command,
    GenerationType.genselect: get_server_command,
}

client_command_factories = {
    GenerationType.generate: get_cmd,
    GenerationType.reward: get_rm_cmd,
    GenerationType.math_judge: get_math_judge_cmd,
    GenerationType.genselect: get_genselect_cmd,
}

client_command_scripts = {
    GenerationType.generate: 'nemo_skills.inference.generate',
    GenerationType.reward: 'nemo_skills.inference.reward_model',
    GenerationType.math_judge: 'nemo_skills.inference.llm_math_judge',
    GenerationType.genselect: 'nemo_skills.inference.genselect',
}


def configure_client(
    generation_type,
    server_gpus,
    server_type,
    server_address,
    server_port,
    server_nodes,
    model,
    server_args,
    server_entrypoint,
    extra_arguments,
):
    if server_address is None:  # we need to host the model
        server_port = get_free_port(strategy="random")
        assert server_gpus is not None, "Need to specify server_gpus if hosting the model"
        server_address = f"localhost:{server_port}"

        server_config = {
            "model_path": model,
            "server_type": server_type,
            "num_gpus": server_gpus,
            "num_nodes": server_nodes,
            "server_args": server_args,
            "server_entrypoint": server_entrypoint,
            "server_port": server_port,
        }
        extra_arguments = (
            f"{extra_arguments} ++server.server_type={server_type} "
            f"++server.host=localhost ++server.port={server_port} "
        )
    else:  # model is hosted elsewhere
        server_config = None
        extra_arguments = (
            f"{extra_arguments} ++server.server_type={server_type} "
            f"++server.base_url={server_address} ++server.model={model} "
        )
        server_port = None
    return server_config, extra_arguments, server_address, server_port


@app.command(context_settings={"allow_extra_args": True, "ignore_unknown_options": True})
@typer_unpacker
def generate(
    ctx: typer.Context,
    cluster: str = typer.Option(
        None,
        help="One of the configs inside config_dir or NEMO_SKILLS_CONFIG_DIR or ./cluster_configs. "
        "Can also use NEMO_SKILLS_CONFIG instead of specifying as argument.",
    ),
    output_dir: str = typer.Option(..., help="Where to put results"),
    expname: str = typer.Option("generate", help="Nemo run experiment name"),
    generation_type: GenerationType = typer.Option(GenerationType.generate, help="Type of generation to perform"),
    model: str = typer.Option(None, help="Path to the model or model name in API"),
    server_address: str = typer.Option(
        None, help="Use ip:port for self-hosted models or the API url if using model providers"
    ),
    server_type: SupportedServers = typer.Option(..., help="Type of server to use"),
    server_gpus: int = typer.Option(None, help="Number of GPUs to use if hosting the model"),
    server_nodes: int = typer.Option(1, help="Number of nodes required for hosting LLM server"),
    server_args: str = typer.Option("", help="Any extra arguments to pass to the server"),
    server_entrypoint: str = typer.Option(
        None,
        help="Path to the entrypoint of the server. "
        "If not specified, will use the default entrypoint for the server type.",
    ),
    dependent_jobs: int = typer.Option(0, help="Specify this to launch that number of dependent jobs"),
    mount_paths: str = typer.Option(None, help="Comma separated list of paths to mount on the remote machine"),
    num_random_seeds: int = typer.Option(
        None, help="Specify if want to run many generations with high temperature for the same input"
    ),
    random_seeds: str = typer.Option(
        None,
        help="List of random seeds to use for generation. Separate with , or .. to specify range. "
        "Can provide a list directly when using through Python",
    ),
    starting_seed: int = typer.Option(0, help="Starting seed for random sampling"),
    num_chunks: int = typer.Option(
        None,
        help="Number of chunks to split the dataset into. If None, will not chunk the dataset.",
    ),
    chunk_ids: str = typer.Option(
        None,
        help="List of explicit chunk ids to run. Separate with , or .. to specify range. "
        "Can provide a list directly when using through Python",
    ),
    preprocess_cmd: str = typer.Option(None, help="Command to run before generation"),
    postprocess_cmd: str = typer.Option(None, help="Command to run after generation"),
    partition: str = typer.Option(
        None, help="Can specify if need interactive jobs or a specific non-default partition"
    ),
    time_min: str = typer.Option(None, help="If specified, will use as a time-min slurm parameter"),
    eval_args: str = typer.Option(
        None, help="Specify if need to run nemo_skills/evaluation/evaluate_results.py on the generation outputs"
    ),
    genselect_args: str = typer.Option(None, help="Can specify extra arguments to prepare the data for genselect"),
    run_after: List[str] = typer.Option(
        None, help="Can specify a list of expnames that need to be completed before this one starts"
    ),
    reuse_code: bool = typer.Option(
        True,
        help="If True, will reuse the code from the provided experiment. "
        "If you use it from Python, by default the code will be re-used from "
        "the last submitted experiment in the current Python session, so set to False to disable "
        "(or provide reuse_code_exp to override).",
    ),
    reuse_code_exp: str = typer.Option(
        None,
        help="If specified, will reuse the code from this experiment. "
        "Can provide an experiment name or an experiment object if running from code.",
    ),
    config_dir: str = typer.Option(None, help="Can customize where we search for cluster configs"),
    log_dir: str = typer.Option(None, help="Can specify a custom location for slurm logs."),
    output_prefix: str = typer.Option(
        "output", help="Optional base name for output .jsonl files. If provided, will be used in place of 'output'."
    ),
    exclusive: bool = typer.Option(
        True,
        "--not_exclusive",
        help="If --not_exclusive is used, will NOT use --exclusive flag for slurm",
    ),
    rerun_done: bool = typer.Option(
        False, help="If True, will re-run jobs even if a corresponding '.done' file already exists"
    ),
    with_sandbox: bool = typer.Option(False, help="If True, will start a sandbox container alongside this job"),
    check_mounted_paths: bool = typer.Option(False, help="Check if mounted paths are available on the remote machine"),
    log_samples: bool = typer.Option(
        False,
        help="If True, will log random samples from the output files to wandb. "
        "Requires WANDB_API_KEY to be set in the environment. "
        "Use expname/wandb_group/wandb_project to specify where to log.",
    ),
    wandb_name: str = typer.Option(
        None,
        help="Name of the wandb group to sync samples to. If not specified, but log_samples=True, will use expname.",
    ),
    wandb_group: str = typer.Option(None, help="Name of the wandb group to sync samples to."),
    wandb_project: str = typer.Option(
        'nemo-skills',
        help="Name of the wandb project to sync samples to.",
    ),
):
    """Generate LLM completions for a given input file.

    Run `python -m nemo_skills.inference.generate --help` for other supported arguments
    (need to be prefixed with ++, since we use Hydra for that script).
    """
    setup_logging(disable_hydra_logs=False, use_rich=True)
    extra_arguments = f'{" ".join(ctx.args)}'

    chunking_enabled = (num_chunks is not None) or (chunk_ids is not None)
    if chunking_enabled and generation_type != GenerationType.generate:
        logging.error(
            "Chunking is enabled, but generation type is not 'generate'. "
            "Chunking is only supported for generation type 'generate'."
            "This may result in superfluous generation jobs."
        )
        raise ValueError("Chunking is only supported for generation type 'generate'")

    try:
        server_type = server_type.value
    except AttributeError:
        pass

    if log_samples:
        wandb_parameters = {
            'name': wandb_name or expname,
            'project': wandb_project,
            'group': wandb_group,
        }
    else:
        wandb_parameters = None

    get_random_port = server_gpus != 8 and not exclusive

    if random_seeds and num_random_seeds:
        raise ValueError("Cannot specify both random_seeds and num_random_seeds")
    if num_random_seeds:
        random_seeds = list(range(starting_seed, starting_seed + num_random_seeds))
    if isinstance(random_seeds, str):
        random_seeds = str_ids_to_list(random_seeds)

    if num_chunks:
        chunk_ids = compute_chunk_ids(chunk_ids, num_chunks)
    if chunk_ids is None:
        chunk_ids = [None]

    # Prepare cluster config and mount paths
    cluster_config = get_cluster_config(cluster, config_dir)
    cluster_config = resolve_mount_paths(cluster_config, mount_paths, create_remote_dir=check_mounted_paths)

    if not log_dir:
        log_dir = f"{output_dir}/generation-logs"

    output_dir, log_dir = check_mounts(
        cluster_config,
        log_dir=log_dir,
        mount_map={output_dir: None},
        check_mounted_paths=check_mounted_paths,
    )

    get_server_command = server_command_factories[generation_type]
    get_cmd = client_command_factories[generation_type]
    cmd_script = client_command_scripts[generation_type]
    original_server_address = server_address

    # If GenerationType is `generate`, check if custom GenerationTask is provided via ctx.obj['generation_task_type']
    if (
        generation_type == GenerationType.generate
        and ctx.obj is not None
        and isinstance(ctx.obj, dict)
        and 'generation_task_type' in ctx.obj
    ):
        generation_task = ctx.obj['generation_task_type']  # type: type(GenerationTask)
        assert issubclass(
            generation_task, GenerationTask
        ), f"`generation_task_type` must be a subclass of GenerationTask"
        cmd_script = generation_task.get_generation_module()
        cmd_extra_args = generation_task.get_generation_default_args()
        cmd_script = f"{cmd_script.strip()} {cmd_extra_args.strip()}"

    extra_arguments_original = extra_arguments

    # Treat no random seeds as a single None seed to unify the code paths
    if not random_seeds:
        random_seeds = [None]

    remaining_jobs = get_remaining_jobs(
        cluster_config=cluster_config,
        output_dir=output_dir,
        random_seeds=random_seeds,
        chunk_ids=chunk_ids,
        rerun_done=rerun_done,
        output_prefix=output_prefix,
    )
    has_tasks = False

    with get_exp(expname, cluster_config) as exp:
        if generation_type == GenerationType.genselect:
            # Add the preprocessing command for genselect
            genselect_args = f" ++num_random_seeds={len(random_seeds)} ++output_dir={output_dir} " + (
                genselect_args if genselect_args is not None else ""
            )
            preprocess_cmd = f"python -m nemo_skills.inference.genselect_preprocess {genselect_args}"

            preprocess_task = add_task(
                exp,
                cmd=preprocess_cmd,
                task_name="preprocess_genselect",
                log_dir=f"{output_dir}/preprocess-logs",
                container=cluster_config["containers"]["nemo-skills"],
                cluster_config=cluster_config,
            )
            initial_tasks = [preprocess_task]

        else:
            initial_tasks = None

        for job_idx, (seed, chunk_ids) in enumerate(remaining_jobs.items()):
            if wandb_parameters:
                # no need for chunks as it will run after merging
                wandb_parameters['samples_file'] = get_chunked_rs_filename(
                    output_dir,
                    random_seed=seed,
                    chunk_id=None,
                    output_prefix=output_prefix,
                )
            for chunk_id in chunk_ids:
                has_tasks = True
                server_port = get_free_port(strategy="random") if get_random_port else 5000
                server_config, extra_arguments, server_address, server_port = configure_client(
                    generation_type=generation_type,
                    server_gpus=server_gpus,
                    server_type=server_type,
                    server_address=original_server_address,
                    server_port=server_port,
                    server_nodes=server_nodes,
                    model=model,
                    server_args=server_args,
                    server_entrypoint=server_entrypoint,
                    extra_arguments=extra_arguments_original,
                )
                cmd, full_postprocess_cmd = get_cmd(
                    random_seed=seed,
                    output_dir=output_dir,
                    extra_arguments=extra_arguments,
                    eval_args=eval_args,
                    chunk_id=chunk_id,
                    num_chunks=num_chunks,
                    output_prefix=output_prefix,
                    postprocess_cmd=postprocess_cmd,
                    script=cmd_script,
                )
                prev_tasks = initial_tasks
                for _ in range(dependent_jobs + 1):
                    task_name = f'{expname}-rs{seed}' if seed is not None else expname
                    if chunk_id is not None:
                        task_name += f'-chunk{chunk_id}'
                    new_task = add_task(
                        exp,
                        cmd=wrap_cmd(
                            get_generation_command(server_address=server_address, generation_commands=cmd),
                            preprocess_cmd,
                            full_postprocess_cmd,
                            random_seed=seed,
                            # only logging for the first job
                            wandb_parameters=wandb_parameters if job_idx == 0 else None,
                        ),
                        task_name=task_name,
                        log_dir=log_dir,
                        container=cluster_config["containers"]["nemo-skills"],
                        cluster_config=cluster_config,
                        partition=partition,
                        time_min=time_min,
                        server_config=server_config,
                        with_sandbox=with_sandbox,
                        sandbox_port=None if get_random_port else 6000,
                        run_after=run_after,
                        reuse_code=reuse_code,
                        reuse_code_exp=reuse_code_exp,
                        task_dependencies=prev_tasks,
                        get_server_command=get_server_command,
                        slurm_kwargs={"exclusive": exclusive} if exclusive else None,
                    )
                    prev_tasks = [new_task]
        if has_tasks:
            run_exp(exp, cluster_config)

    if has_tasks:
        return exp
    return None


if __name__ == "__main__":
    typer.main.get_command_name = lambda name: name
    app()
