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
import multiprocessing
import resource
import sys
from io import StringIO
import traceback

from flask import Flask, request

app = Flask(__name__)


# need to memory-limit to avoid common errors of allocating too much
# but this has to be done in a subprocess to not crush server itself
def execute_code_subprocess(generated_code, queue):
    # Set memory limits to avoid crashing the server
    limit = 1024 * 1024 * 1024 * 10  # 10 GB limit
    resource.setrlimit(resource.RLIMIT_AS, (limit, limit))
    resource.setrlimit(resource.RLIMIT_DATA, (limit, limit))
    resource.setrlimit(resource.RLIMIT_STACK, (limit, limit))

    stdout_2 = StringIO()
    stderr_2 = StringIO()

    try:
        # Capture any stdout or stderr from exec()
        exec(generated_code, {'stdout_2': stdout_2}, {})
        queue.put({"stdout": stdout_2.getvalue(), "stderr": ""})
    except Exception as e:
        # Capture any exception, including SyntaxError, and return it as stderr
        stderr_2.write(f"{type(e).__name__}: {str(e)}")
        queue.put({"stdout": stdout_2.getvalue(), "stderr": "\n".join(traceback.format_exc().split("\n")[3:])})



@app.route("/execute", methods=["POST"])
def execute():
    generated_code = request.json['generated_code']
    timeout = request.json['timeout']
    queue = multiprocessing.Queue()
    process = multiprocessing.Process(target=execute_code_subprocess, args=(generated_code, queue))
    process.start()
    process.join(timeout=timeout)
    
    if process.is_alive():  # didn't finish successfully within the timeout
        process.kill()
        return '{"process_status": "timeout", "stdout": "Timed out", "stderr": "Timed out"}'
    
    result = queue.get()  # Fetch the result from the subprocess
    stdout = result.get("stdout", "")
    stderr = result.get("stderr", "")
    
    return {"process_status": "completed", "stdout": stdout, "stderr": stderr}


def execute_code_inline(generated_code):
    sys.stdout = StringIO()
    sys.stderr = StringIO()
    exec(generated_code, {})
    return {'stdout': sys.stdout.getvalue(), 'stderr': sys.stderr.getvalue()}

@app.route("/execute_fast", methods=["POST"])
def execute_fast():
    generated_code = request.json['generated_code']
    try:
        result = execute_code_inline(generated_code)
        return result
    except Exception as e:
        return {'stdout': str(e), 'stderr': str(e)}
        # return str(e)