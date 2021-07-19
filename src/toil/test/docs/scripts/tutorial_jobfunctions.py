import tempfile

from toil.common import Toil
from toil.job import Job

def helloWorld(job, message):
    job.log("Hello world, I have a message: {}".format(message))

if __name__=="__main__":
    options = Job.Runner.getDefaultOptions(tempfile.mkdtemp("tutorial_jobfunctions"))
    options.logLevel = "INFO"
    options.clean = "always"

    hello_job = Job.wrapJobFn(helloWorld, "Woot!")

    with Toil(options) as toil:
        toil.start(hello_job)
