import os
import tempfile

from toil.common import Toil
from toil.job import Job
from toil.lib.docker import apiDockerCall

align = Job.wrapJobFn(apiDockerCall,
                      image='ubuntu',
                      working_dir=os.getcwd(),
                      parameters=['ls', '-lha'])

if __name__=="__main__":
    options = Job.Runner.getDefaultOptions(tempfile.mkdtemp("tutorial_docker")+os.sep+"toilWorkflowRun")
    options.logLevel = "INFO"
    options.clean = "always"

    with Toil(options) as toil:
       toil.start(align)
