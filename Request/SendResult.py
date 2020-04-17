from Request.Request import Request

class SendResult(Request):

    def __init__(self, job_id, job):
        super().__init__(message_type="sendResult")
        self.job_id = job_id
        self.job = job

    def getJobId_Job(self):
        return self.job_id, self.job