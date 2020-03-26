import Request.Request
import random

class JobRequest(Request):

    def __init__(self):
        Request.__init__(self, type="jobRequest")
        self.job_delay = random.randrange(10,30)

    def get_job_delay(self):
        return self.job_delay