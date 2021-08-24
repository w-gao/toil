import json
import os
import subprocess
import logging
import shutil

from typing import List, Union

from toil.server.wes.abstractBackend import WESBackend, get_iso_time

logging.basicConfig(level=logging.INFO)


class ToilWorkflow:
    def __init__(self, run_id: str):
        """
        Represents a toil workflow.

        :param run_id: A uuid string.  Used to name the folder that contains
                       all of the files containing this particular workflow
                       instance's information.
        """
        self.run_id = run_id

        self.work_dir = os.path.join(os.getcwd(), "workflows", self.run_id)
        self.out_dir = os.path.join(self.work_dir, "outdir")
        if not os.path.exists(self.out_dir):
            os.makedirs(self.out_dir)

        # execution
        self.stdout_file = os.path.join(self.work_dir, "stdout")
        self.stderr_file = os.path.join(self.work_dir, "stderr")

        self.start_time = os.path.join(self.work_dir, "starttime")
        self.end_time = os.path.join(self.work_dir, "endtime")

        self.exit_code_file = os.path.join(self.work_dir, "exit_code")
        self.stat_complete_file = os.path.join(self.work_dir, "status_completed")
        self.stat_error_file = os.path.join(self.work_dir, "status_error")

        # workflow information
        self.cmd_file = os.path.join(self.work_dir, "cmd")
        self.job_store_file = os.path.join(self.work_dir, "jobstore")

        self.request_json = os.path.join(self.work_dir, "request.json")
        self.input_json = os.path.join(self.work_dir, "wes_input.json")

        self.job_store_default = "file:" + os.path.join(self.work_dir, "toiljobstore")
        self.job_store = None

    def sort_toil_options(self, options: List[str]):
        """
        """
        # determine job store and set a new default if the user did not set one
        # cloud = False
        for e in options:
            if e.startswith("--jobStore="):
                self.job_store = e[11:]
                # if self.job_store.startswith(("aws", "google", "azure")):
                #     cloud = True
                options.remove(e)
            if e.startswith(("--outdir=", "-o=")):
                options.remove(e)

        # TODO: --outdir is only valid for cwl and wdl workflows...
        # if not cloud:
        #     options.append("--outdir=" + self.out_dir)
        if not self.job_store:
            self.job_store = self.job_store_default
        options.append(self.job_store)  # append the positional jobStore argument at the end

        # store the job store location
        with open(self.job_store_file, "w") as f:
            f.write(self.job_store)

        return options

    def write_workflow(self, request, opts, cwd, wftype="cwl"):
        """
        Writes a cwl, wdl, or python file as appropriate from the request
        dictionary.
        """

        workflow_url = request.get("workflow_url")

        # link the cwl and json into the cwd
        if workflow_url.startswith("file://"):
            try:
                os.link(workflow_url[7:], os.path.join(cwd, "wes_workflow." + wftype))
            except OSError:
                os.symlink(workflow_url[7:], os.path.join(cwd, "wes_workflow." + wftype))
            workflow_url = os.path.join(cwd, "wes_workflow." + wftype)
        try:
            os.link(self.input_json, os.path.join(cwd, "wes_input.json"))
        except OSError:
            os.symlink(self.input_json, os.path.join(cwd, "wes_input.json"))
        self.input_json = os.path.join(cwd, "wes_input.json")

        extra_options = self.sort_toil_options(opts.getoptlist("extra"))
        if wftype == "cwl":
            command_args = (
                ["toil-cwl-runner"] + extra_options + [workflow_url, self.input_json]
            )
        elif wftype == "wdl":
            command_args = (
                ["toil-wdl-runner"] + extra_options + [workflow_url, self.input_json]
            )
        elif wftype == "py":
            command_args = ["python"] + [workflow_url] + extra_options
        else:
            raise RuntimeError(
                'workflow_type is not "cwl", "wdl", or "py": ' + str(wftype)
            )

        return command_args

    def write_json(self, request_dict):
        input_json = os.path.join(self.work_dir, "input.json")
        with open(input_json, "w") as f:
            json.dump(request_dict["workflow_params"], f)
        return input_json

    def call_cmd(self, cmd: Union[List[str], str], cwd: str) -> int:
        """
        Calls a command with Popen. Writes stdout, stderr, and the command to
        separate files.

        This is a blocking call.

        :param cmd: A string or array of strings.
        :param cwd: A path to the working directory.

        :return: The exit code of the command.
        """
        with open(self.cmd_file, "w") as f:
            f.write(' '.join(cmd))

        with open(self.stdout_file, "w") as stdout, open(self.stderr_file, "w") as stderr:
            logging.info("Calling: " + " ".join(cmd))
            process = subprocess.Popen(cmd,
                                       stdout=stdout,
                                       stderr=stderr,
                                       close_fds=True,
                                       cwd=cwd)

        return process.wait()

    def cancel(self):
        pass

    def fetch(self, filename: str, default_value: str = "") -> str:
        """
        Returns the contents of the given file. If the file does not exist, the
        default value is returned.
        """
        if os.path.exists(filename):
            with open(filename, "r") as f:
                return f.read()
        return default_value

    def get_log(self) -> dict:
        """
        """
        state, exit_code = self.getstate()

        with open(self.request_json, "r") as f:
            request = json.load(f)

        with open(self.job_store_file, "r") as f:
            self.job_store = f.read()

        stderr = self.fetch(self.stderr_file)
        start_time = self.fetch(self.start_time)
        end_time = self.fetch(self.end_time)
        cmd = [self.fetch(self.cmd_file)]

        output_obj = {}
        if state == "COMPLETE":
            # only tested locally
            if self.job_store.startswith("file:"):
                for f in os.listdir(self.out_dir):
                    if f.startswith("out_tmpdir"):
                        shutil.rmtree(os.path.join(self.out_dir, f))
                for f in os.listdir(self.out_dir):
                    output_obj[f] = {
                        "location": os.path.join(self.out_dir, f),
                        "size": os.stat(os.path.join(self.out_dir, f)).st_size,
                        "class": "File",
                    }

        return {
            "run_id": self.run_id,
            "request": request,
            "state": state,
            "run_log": {
                "cmd": cmd,
                "start_time": start_time,
                "end_time": end_time,
                "stdout": "",
                "stderr": stderr,
                "exit_code": exit_code,
            },
            "task_logs": [],
            "outputs": output_obj,
        }

    def run(self, request, tempdir, opts):
        """
        Constructs a command to run a cwl/json from requests and opts,
        runs it, and deposits the outputs in outdir.

        Runner:
        opts.getopt("runner", default="cwl-runner")

        CWL (url):
        request["workflow_url"] == a url to a cwl file
        or
        request["workflow_attachment"] == input cwl text (written to a file and a url constructed for that file)

        JSON File:
        request["workflow_params"] == input json text (to be written to a file)

        :param dict request: A dictionary containing the cwl/json information.
        :param str tempdir: Folder where input files have been staged and the cwd to run at.
        :param wes_service.util.WESBackend opts: contains the user's arguments;
                                                 specifically the runner and runner options
        :return: {"run_id": self.run_id, "state": state}
        """
        wftype = request["workflow_type"].lower().strip()
        version = request["workflow_type_version"]

        if version != "v1.0" and wftype == "cwl":
            raise RuntimeError(
                'workflow_type "cwl" requires '
                '"workflow_type_version" to be "v1.0": ' + str(version)
            )
        if version != "2.7" and wftype == "py":
            raise RuntimeError(
                'workflow_type "py" requires '
                '"workflow_type_version" to be "2.7": ' + str(version)
            )

        logging.info("Beginning Toil Workflow ID: " + str(self.run_id))

        with open(self.start_time, "w") as f:
            f.write(get_iso_time())
        with open(self.request_json, "w") as f:
            json.dump(request, f)
        with open(self.input_json, "w") as temp:
            json.dump(request["workflow_params"], temp)

        command_args = self.write_workflow(request, opts, tempdir, wftype=wftype)
        exit_code = self.call_cmd(command_args, tempdir)

        with open(self.end_time, "w") as f:
            f.write(get_iso_time())
        with open(self.exit_code_file, "w") as f:
            f.write(str(exit_code))

        return self.getstatus()

    def getstate(self):
        """
        Returns QUEUED,          -1
                INITIALIZING,    -1
                RUNNING,         -1
                COMPLETE,         0
                or
                EXECUTOR_ERROR, 255
        """
        # the job store never existed
        if not os.path.exists(self.job_store_file):
            logging.info("Workflow " + self.run_id + ": QUEUED")
            return "QUEUED", -1

        # completed earlier
        if os.path.exists(self.stat_complete_file):
            logging.info("Workflow " + self.run_id + ": COMPLETE")
            return "COMPLETE", 0

        # errored earlier
        if os.path.exists(self.stat_error_file):
            logging.info("Workflow " + self.run_id + ": EXECUTOR_ERROR")
            return "EXECUTOR_ERROR", 255

        # the workflow is staged but has not run yet
        if not os.path.exists(self.stderr_file):
            logging.info("Workflow " + self.run_id + ": INITIALIZING")
            return "INITIALIZING", -1

        # TODO: Query with "toil status"
        completed = False
        with open(self.stderr_file, "r") as f:
            for line in f:
                # TODO: not all failed exits have this message (e.g.: when wrong arguments provided)
                if "Traceback (most recent call last)" in line:
                    logging.info("Workflow " + self.run_id + ": EXECUTOR_ERROR")
                    open(self.stat_error_file, "a").close()
                    return "EXECUTOR_ERROR", 255
                # run can complete successfully but fail to upload outputs to cloud buckets
                # so save the completed status and make sure there was no error elsewhere
                if "Finished toil run successfully." in line:
                    completed = True
        if completed:
            logging.info("Workflow " + self.run_id + ": COMPLETE")
            open(self.stat_complete_file, "a").close()
            return "COMPLETE", 0

        logging.info("Workflow " + self.run_id + ": RUNNING")
        return "RUNNING", -1

    def getstatus(self):
        state, exit_code = self.getstate()

        return {"run_id": self.run_id, "state": state}
