from abc import abstractmethod
from typing import Set


class WorkflowRunner:
    """
    """
    @classmethod
    @abstractmethod
    def supported_versions(cls) -> Set[str]:
        """
        Get all the workflow versions that this runner implementation supports.
        """
        raise NotImplementedError


class PythonRunner(WorkflowRunner):
    """
    python
    """
    @classmethod
    def supported_versions(cls) -> Set[str]:
        return {"3.6", "3.7", "3.8", "3.9"}


class CWLRunner(WorkflowRunner):
    """
    toil-cwl-runner
    """
    @classmethod
    def supported_versions(cls) -> Set[str]:
        return {"v1.0", "v1.1", "v1.2"}


class WDLRunner(WorkflowRunner):
    """
    toil-wdl-runner
    """
    @classmethod
    def supported_versions(cls) -> Set[str]:
        return {"draft-2", "1.0"}


def get_workflow_runners():
    return {
        "python": PythonRunner,
        "cwl": CWLRunner,
        "wdl": WDLRunner
    }

