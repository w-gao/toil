from abc import abstractmethod
from typing import List


class WorkflowRunner:
    """
    """
    @classmethod
    @abstractmethod
    def supported_versions(cls) -> List[str]:
        """
        Get all the workflow versions that this runner implementation supports.
        """
        raise NotImplementedError


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


WES_WORKFLOW_RUNNERS = {
    "py": PythonRunner,
    "cwl": CWLRunner,
    "wdl": WDLRunner
}
