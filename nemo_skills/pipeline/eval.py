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
import logging
import os
from pathlib import Path
from typing import List

import typer

from nemo_skills.dataset.utils import ExtraDatasetType, get_dataset_module
from nemo_skills.pipeline.app import app, typer_unpacker
from nemo_skills.pipeline.utils import (
    SupportedServers,
    add_task,
    check_mounts,
    cluster_path_exists,
    get_cluster_config,
    get_env_variables,
    get_exp,
    get_free_port,
    get_generation_command,
    get_server_command,
    get_unmounted_path,
    is_mounted_filepath,
    resolve_mount_paths,
    run_exp,
)
from nemo_skills.utils import compute_chunk_ids, get_chunked_filename, get_logger_name, setup_logging

LOG = logging.getLogger(get_logger_name(__file__))


def get_greedy_cmd(
    benchmark,
    output_dir,
    output_name='output.jsonl',
    extra_eval_args="",
    extra_arguments="",
    num_chunks=None,
    chunk_ids=None,
):
    cmds = []
    if num_chunks is None or chunk_ids is None:
        chunk_params = ["++chunk_id=null ++num_chunks=null"]
        chunked_output_names = [output_name]
    else:
        chunk_params = [f"++chunk_id={chunk_id} ++num_chunks={num_chunks}" for chunk_id in chunk_ids]
        chunked_output_names = [get_chunked_filename(chunk_id, output_name) for chunk_id in chunk_ids]
    for chunk_param, chunked_output_name in zip(chunk_params, chunked_output_names):
        cmds.append(
            f'echo "Evaluating benchmark {benchmark}" && '
            f'python -m nemo_skills.inference.generate '
            f'    ++output_file={output_dir}/eval-results/{benchmark}/{output_name} '
            f'    {chunk_param} '
            f'    {extra_arguments} && '
            f'python -m nemo_skills.evaluation.evaluate_results '
            f'    ++input_files={output_dir}/eval-results/{benchmark}/{chunked_output_name} {extra_eval_args}'
        )
    return cmds


def get_sampling_cmd(
    benchmark,
    output_dir,
    random_seed,
    extra_eval_args="",
    extra_arguments="",
    num_chunks=None,
    chunk_ids=None,
):
    extra_arguments = f" inference.random_seed={random_seed} inference.temperature=0.7 {extra_arguments}"
    return get_greedy_cmd(
        benchmark=benchmark,
        output_dir=output_dir,
        output_name=f"output-rs{random_seed}.jsonl",
        extra_eval_args=extra_eval_args,
        extra_arguments=extra_arguments,
        num_chunks=num_chunks,
        chunk_ids=chunk_ids,
    )


def add_default_args(
    cluster_config, benchmark, split, data_dir, extra_eval_args, extra_arguments, extra_datasets_type, extra_datasets
):
    benchmark_module, data_path, is_on_cluster = get_dataset_module(
        dataset=benchmark,
        data_dir=data_dir,
        cluster_config=cluster_config,
        extra_datasets=extra_datasets,
        extra_datasets_type=extra_datasets_type,
    )
    benchmark = benchmark.replace('.', '/')

    if split is None:
        split = getattr(benchmark_module, "EVAL_SPLIT", "test")
    if not is_on_cluster:
        if is_mounted_filepath(cluster_config, data_path):
            input_file = f"{data_path}/{benchmark}/{split}.jsonl"
            unmounted_input_file = get_unmounted_path(cluster_config, input_file)
            unmounted_path = str(Path(__file__).parents[2] / unmounted_input_file.replace('/nemo_run/code/', ''))
        else:
            # will be copied over in this case as it must come from extra datasets
            input_file = f"/nemo_run/code/{Path(data_path).name}/{benchmark}/{split}.jsonl"
            unmounted_path = Path(data_path) / benchmark / f"{split}.jsonl"
    else:
        # on cluster we will always use the mounted path
        input_file = f"{data_path}/{benchmark}/{split}.jsonl"
        unmounted_path = get_unmounted_path(cluster_config, input_file)

    unmounted_path = str(unmounted_path)
    # checking if data file exists (can check locally as well)
    if is_on_cluster:
        if not cluster_path_exists(cluster_config, unmounted_path):
            raise ValueError(
                f"Data file {unmounted_path} does not exist on cluster. "
                "Please check the benchmark and split parameters. "
                "Did you forget to run prepare data commands?"
            )
    else:
        if not Path(unmounted_path).exists():
            raise ValueError(
                f"Data file {unmounted_path} does not exist locally. "
                "Please check the benchmark and split parameters. "
                "Did you forget to run prepare data commands?"
            )

    extra_eval_args = f"{benchmark_module.EVAL_ARGS} {extra_eval_args}"
    prompt_config_arg = f"++prompt_config={benchmark_module.PROMPT_CONFIG}"
    default_arguments = f"++input_file={input_file} {prompt_config_arg} {benchmark_module.GENERATION_ARGS}"
    extra_arguments = f"{default_arguments} {extra_arguments}"

    requires_sandbox = hasattr(benchmark_module, "DATASET_GROUP") and benchmark_module.DATASET_GROUP == "lean4"

    return extra_arguments, extra_eval_args, requires_sandbox


@app.command(context_settings={"allow_extra_args": True, "ignore_unknown_options": True})
@typer_unpacker
def eval(
    ctx: typer.Context,
    cluster: str = typer.Option(
        None,
        help="One of the configs inside config_dir or NEMO_SKILLS_CONFIG_DIR or ./cluster_configs. "
        "Can also use NEMO_SKILLS_CONFIG instead of specifying as argument.",
    ),
    output_dir: str = typer.Option(..., help="Where to store evaluation results"),
    data_dir: str = typer.Option(
        None,
        help="Path to the data directory. If not specified, will use the default nemo_skills/dataset path. "
        "Can also specify through NEMO_SKILLS_DATA_DIR environment variable.",
    ),
    benchmarks: str = typer.Option(
        ...,
        help="Need to be in a format <benchmark>:<num samples for majority voting>. "
        "Use <benchmark>:0 to only run greedy decoding. Has to be comma-separated "
        "if providing multiple benchmarks. E.g. gsm8k:4,human-eval:0",
    ),
    expname: str = typer.Option("eval", help="Name of the experiment"),
    model: str = typer.Option(None, help="Path to the model to be evaluated"),
    server_address: str = typer.Option(None, help="Address of the server hosting the model"),
    server_type: SupportedServers = typer.Option(..., help="Type of server to use"),
    server_gpus: int = typer.Option(None, help="Number of GPUs to use if hosting the model"),
    server_nodes: int = typer.Option(1, help="Number of nodes to use if hosting the model"),
    server_args: str = typer.Option("", help="Additional arguments for the server"),
    server_entrypoint: str = typer.Option(
        None,
        help="Path to the entrypoint of the server. "
        "If not specified, will use the default entrypoint for the server type.",
    ),
    starting_seed: int = typer.Option(0, help="Starting seed for random sampling"),
    split: str = typer.Option(
        None,
        help="Data split to use for evaluation. Will use benchmark-specific default or 'test' if it's not defined.",
    ),
    num_jobs: int = typer.Option(-1, help="Number of jobs to split the evaluation into"),
    num_chunks: int = typer.Option(
        None,
        help="Number of chunks to split the dataset into. If None, will not chunk the dataset.",
    ),
    chunk_ids: str = typer.Option(
        None,
        help="List of explicit chunk ids to run. Separate with , or .. to specify range. "
        "Can provide a list directly when using through Python",
    ),
    partition: str = typer.Option(None, help="Cluster partition to use"),
    time_min: str = typer.Option(None, help="If specified, will use as a time-min slurm parameter"),
    mount_paths: str = typer.Option(None, help="Comma separated list of paths to mount on the remote machine"),
    extra_eval_args: str = typer.Option("", help="Additional arguments for evaluation"),
    add_greedy: bool = typer.Option(
        False,
        help="Whether to add greedy evaluation. Only applicable if num_samples > 0, otherwise greedy is default.",
    ),
    run_after: List[str] = typer.Option(
        None, help="Can specify a list of expnames that need to be completed before this one starts"
    ),
    reuse_code_exp: str = typer.Option(
        None,
        help="If specified, will reuse the code from this experiment. "
        "Can provide an experiment name or an experiment object if running from code.",
    ),
    reuse_code: bool = typer.Option(
        True,
        help="If True, will reuse the code from the provided experiment. "
        "If you use it from Python, by default the code will be re-used from "
        "the last submitted experiment in the current Python session, so set to False to disable "
        "(or provide reuse_code_exp to override).",
    ),
    config_dir: str = typer.Option(None, help="Can customize where we search for cluster configs"),
    log_dir: str = typer.Option(None, help="Can specify a custom location for slurm logs."),
    extra_datasets: str = typer.Option(
        None,
        help="Path to a custom dataset folder that will be searched in addition to the main one. "
        "Can also specify through NEMO_SKILLS_EXTRA_DATASETS.",
    ),
    extra_datasets_type: ExtraDatasetType = typer.Option(
        "local",
        envvar="NEMO_SKILLS_EXTRA_DATASETS_TYPE",
        help="If you have extra datasets locally, set to 'local', if on cluster, set to 'cluster'."
        "Can also specify through NEMO_SKILLS_EXTRA_DATASETS_TYPE environment variable.",
    ),
    exclusive: bool = typer.Option(
        True,
        "--not_exclusive",
        help="If --not_exclusive is used, will NOT use --exclusive flag for slurm",
    ),
    with_sandbox: bool = typer.Option(False, help="If True, will start a sandbox container alongside this job"),
    check_mounted_paths: bool = typer.Option(False, help="Check if mounted paths are available on the remote machine"),
):
    """Evaluate a model on specified benchmarks.

    Run `python -m nemo_skills.inference.generate --help` for other supported arguments
    (need to be prefixed with ++, since we use Hydra for that script).
    """
    setup_logging(disable_hydra_logs=False, use_rich=True)
    extra_arguments = f'{" ".join(ctx.args)}'
    LOG.info("Starting evaluation job")
    LOG.info("Extra arguments that will be passed to the underlying script: %s", extra_arguments)

    try:
        server_type = server_type.value
    except AttributeError:
        pass
    try:
        extra_datasets_type = extra_datasets_type.value
    except AttributeError:
        pass

    cluster_config = get_cluster_config(cluster, config_dir)
    cluster_config = resolve_mount_paths(cluster_config, mount_paths)

    env_vars = get_env_variables(cluster_config)
    data_dir = data_dir or env_vars.get("NEMO_SKILLS_DATA_DIR") or os.environ.get("NEMO_SKILLS_DATA_DIR")

    if extra_datasets_type == ExtraDatasetType.cluster and cluster_config['executor'] != 'slurm':
        raise ValueError(
            "Extra datasets type is set to 'cluster', but the executor is not 'slurm'. "
            "Please use 'local' or change the cluster config."
        )

    if log_dir is None:
        log_dir = f"{output_dir}/eval-logs"

    output_dir, data_dir, log_dir = check_mounts(
        cluster_config,
        log_dir=log_dir,
        mount_map={output_dir: None, data_dir: None},
        check_mounted_paths=check_mounted_paths,
    )

    if num_chunks:
        chunk_ids = compute_chunk_ids(chunk_ids, num_chunks)
    should_chunk_dataset = num_chunks is not None and chunk_ids is not None
    num_runs = len(chunk_ids) if should_chunk_dataset else 1

    if " " in str(benchmarks):
        raise ValueError("benchmarks should be separated with commas")

    get_random_port = server_gpus != 8 and not exclusive

    if server_address is None:  # we need to host the model
        assert server_gpus is not None, "Need to specify server_gpus if hosting the model"
        server_port = get_free_port(strategy="random") if get_random_port else 5000
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
        # += is okay here because the args have already been copied in this context
        extra_arguments += f" ++server.server_type={server_type} "
        extra_arguments += f" ++server.host=localhost "
        extra_arguments += f" ++server.port={server_port} "
    else:  # model is hosted elsewhere
        server_config = None
        extra_arguments += (
            f" ++server.server_type={server_type} ++server.base_url={server_address} ++server.model={model} "
        )

    benchmarks = {k: int(v) for k, v in [b.split(":") for b in benchmarks.split(",")]}
    extra_datasets = extra_datasets or os.environ.get("NEMO_SKILLS_EXTRA_DATASETS")

    eval_cmds = []
    benchmark_requires_sandbox = {}

    for benchmark, rs_num in benchmarks.items():
        bench_gen_args, bench_eval_args, requires_sandbox = add_default_args(
            cluster_config,
            benchmark,
            split,
            data_dir,
            extra_eval_args,
            extra_arguments,
            extra_datasets_type,
            extra_datasets,
        )
        benchmark_requires_sandbox[benchmark] = requires_sandbox
        if requires_sandbox and not with_sandbox:
            LOG.warning("Found benchmark (%s) which requires sandbox mode, enabled sandbox for it.", benchmark)

        if add_greedy or rs_num == 0:
            if rs_num > 0:
                # forcing temperature to 0.0 for greedy decoding, but respecting override for samples
                greedy_gen_args = f"{bench_gen_args} ++inference.temperature=0.0"
            else:
                greedy_gen_args = bench_gen_args
            for cmd in get_greedy_cmd(
                benchmark,
                output_dir,
                extra_eval_args=bench_eval_args,
                extra_arguments=greedy_gen_args,
                num_chunks=num_chunks,
                chunk_ids=chunk_ids,
            ):
                eval_cmds.append((cmd, benchmark))
        for rs in range(starting_seed, starting_seed + rs_num):
            for cmd in get_sampling_cmd(
                benchmark,
                output_dir,
                rs,
                extra_eval_args=bench_eval_args,
                extra_arguments=bench_gen_args,
                num_chunks=num_chunks,
                chunk_ids=chunk_ids,
            ):
                eval_cmds.append((cmd, benchmark))

    if num_jobs == -1:
        num_jobs = len(eval_cmds)
    else:
        # TODO: should we keep num_jobs as the total max?
        num_jobs *= num_runs

    # Create job batches with benchmark info
    job_batches = []
    for i in range(num_jobs):
        cmds = []
        benchmarks_in_job = set()
        for cmd, benchmark in eval_cmds[i::num_jobs]:
            cmds.append(cmd)
            benchmarks_in_job.add(benchmark)
        job_batches.append((cmds, benchmarks_in_job))

    with get_exp(expname, cluster_config) as exp:
        for idx, (cmds, benchmarks_in_job) in enumerate(job_batches):
            # Check if any benchmark in this job requires sandbox
            job_needs_sandbox = with_sandbox or any(
                benchmark_requires_sandbox.get(b, False) for b in benchmarks_in_job
            )

            LOG.info("Launching task with command %s", " && ".join(cmds))
            should_package_extra_datasets = extra_datasets and extra_datasets_type == ExtraDatasetType.local
            add_task(
                exp,
                cmd=get_generation_command(server_address=server_address, generation_commands=" && ".join(cmds)),
                task_name=f'{expname}-{idx}',
                log_dir=log_dir,
                container=cluster_config["containers"]["nemo-skills"],
                cluster_config=cluster_config,
                partition=partition,
                time_min=time_min,
                server_config=server_config,
                with_sandbox=job_needs_sandbox,
                run_after=run_after,
                reuse_code_exp=reuse_code_exp,
                reuse_code=reuse_code,
                extra_package_dirs=[extra_datasets] if should_package_extra_datasets else None,
                get_server_command=get_server_command,
                sandbox_port=None if get_random_port else 6000,
                slurm_kwargs={"exclusive": exclusive} if exclusive else None,
            )
        run_exp(exp, cluster_config)

    return exp


if __name__ == "__main__":
    # workaround for https://github.com/fastapi/typer/issues/341
    typer.main.get_command_name = lambda name: name
    app()
