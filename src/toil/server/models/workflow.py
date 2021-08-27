import os
from abc import abstractmethod
from typing import List


class WESWorkflow:
    """
    Represents a WES workflow of a particular type. Intended to be inherited.
    This class is responsible for generating a shell command that runs the
    requested workflow.
    """

    def __init__(self,
                 run_id: str,
                 work_dir: str,
                 workflow_url: str,
                 input_json: str,
                 options: List[str]) -> None:
        """
        :param run_id: The run id.
        :param work_dir: The directory to store workflow data.
        :param workflow_url: The URL to the workflow file.
        :param input_json: The URL to the input JSON file.
        :param options: A list of user defined options that should be appended
                        to the shell command run.
        """
        self.run_id = run_id
        self.cloud = False

        self.workflow_url = workflow_url
        self.input_json = input_json
        self.options = options

        self.temp_dir = os.path.join(work_dir, "run_dir")
        self.out_dir = os.path.join(work_dir, "out_dir")

        default_job_store = "file:" + os.path.join(work_dir, "toiljobstore")
        self.job_store: str = default_job_store

    @classmethod
    def supported_versions(cls) -> List[str]:
        """
        Get all the workflow versions that this runner implementation supports.
        """
        raise NotImplementedError

    def prepare_run(self) -> None:
        """ Prepare the workflow run."""
        self.link_files()
        self.sort_options()

    @staticmethod
    def _link_file(src: str, dest: str) -> None:
        try:
            os.link(src, dest)
        except OSError:
            os.symlink(src, dest)

    def link_files(self) -> None:
        """
        Link the workflow and its JSON input file to the self.temp_dir.
        """

        # link the workflow file into the cwd
        if self.workflow_url.startswith("file://"):
            dest = os.path.join(self.temp_dir, "wes_workflow" + os.path.splitext(self.workflow_url)[1])
            self._link_file(src=self.workflow_url[7:], dest=dest)
            self.workflow_url = dest

        # link the JSON file into the cwd
        dest = os.path.join(self.temp_dir, "wes_input.json")
        self._link_file(src=self.input_json, dest=dest)
        self.input_json = dest

    def sort_options(self) -> None:
        """
        Given the user request, sort the command line arguments in the order
        that can be recognized by the runner.
        """

        # determine job store and set a new default if the user did not set one
        for opt in self.options:
            if opt.startswith("--jobStore="):
                self.job_store = opt[11:]
                if self.job_store.startswith(("aws", "google", "azure")):
                    self.cloud = True
                self.options.remove(opt)
            if opt.startswith(("--outdir=", "-o=")):
                # remove user-defined output directories
                self.options.remove(opt)

        self._sort_options()

    @abstractmethod
    def _sort_options(self) -> None:
        raise NotImplementedError

    def construct_command(self) -> List[str]:
        """
        Return a list of shell commands that should be executed in order to
        complete this workflow run.
        """
        raise NotImplementedError


class PythonWorkflow(WESWorkflow):
    """ Toil workflows."""

    @classmethod
    def supported_versions(cls) -> List[str]:
        return ["3.6", "3.7", "3.8", "3.9"]

    def _sort_options(self) -> None:
        if not self.cloud:
            # TODO: find a way to communicate the out_dir to the Toil workflow
            pass

        # append the positional jobStore argument at the end for Toil workflows
        self.options.append(self.job_store)

    def construct_command(self) -> List[str]:
        return (
                ["python"] + [self.workflow_url] + self.options
        )


class CWLWorkflow(WESWorkflow):
    """ CWL workflows that are run with toil-cwl-runner."""

    @classmethod
    def supported_versions(cls) -> List[str]:
        return ["v1.0", "v1.1", "v1.2"]

    def _sort_options(self) -> None:
        if not self.cloud:
            self.options.append("--outdir=" + self.out_dir)
        self.options.append("--jobStore=" + self.job_store)

    def construct_command(self) -> List[str]:
        return (
                ["toil-cwl-runner"] + self.options + [self.workflow_url, self.input_json]
        )


class WDLWorkflow(CWLWorkflow):
    """ WDL workflows that are run with toil-wdl-runner."""

    @classmethod
    def supported_versions(cls) -> List[str]:
        return ["draft-2", "1.0"]

    def construct_command(self) -> List[str]:
        return (
                ["toil-wdl-runner"] + self.options + [self.workflow_url, self.input_json]
        )
