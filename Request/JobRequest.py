from Request.Request import Request
import random
import Job


class JobRequest(Request):

    def __init__(self, job):
        super().__init__(message_type='jobRequest')
        self.requested_job = job   # create the job to execute
        self.job_delay = random.randrange(10,30)

    def get_job_delay(self):
        return self.job_delay