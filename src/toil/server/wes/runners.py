from typing import List, Dict

from toil.server.wes.abstractBackend import DefaultOptions


class WorkflowRunner:
    """
    Represents a workflow runner that runs the requested workflow.
    """
    @classmethod
    def supported_versions(cls) -> List[str]:
        """
        Get all the workflow versions that this runner implementation supports.
        """
        raise NotImplementedError

    def sort_options(self, temp_dir: str, out_dir: str, request: Dict[str, str], options: DefaultOptions) -> List[str]:
        """
        """

    def build_command(self) -> None:
        """
        """


class PythonRunner(WorkflowRunner):
    @classmethod
    def supported_versions(cls) -> List[str]:
        return ["3.6", "3.7", "3.8", "3.9"]


class CWLRunner(WorkflowRunner):
    @classmethod
    def supported_versions(cls) -> List[str]:
        return ["v1.0", "v1.1", "v1.2"]


class WDLRunner(WorkflowRunner):
    @classmethod
    def supported_versions(cls) -> List[str]:
        return ["draft-2", "1.0"]
