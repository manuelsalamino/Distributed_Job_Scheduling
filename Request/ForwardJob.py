from Request.Request import Request
import Job

class ForwardJob(Request):

    def __init__(self, dict_of_jobs):
        super().__init__(message_type="forwardJob")
        self.forwarding_job = dict_of_jobs

    def get_forwarding_job(self):
        return self.forwarding_job