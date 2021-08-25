from time import sleep

from toil.common import Toil
from toil.job import Job


def helloWorld(message: str, memory: str = "1G", cores: int = 1, disk: str = "1G") -> str:
    sleep(20)
    return "Hello, world!, here's a message: %s" % message


if __name__ == "__main__":
    parser = Job.Runner.getDefaultArgumentParser()
    options = parser.parse_args()
    options.clean = "always"
    with Toil(options) as toil:
        output = toil.start(Job.wrapFn(helloWorld, "You did it!"))
    print(output)
