from Request.Request import Request


class ResultRequest(Request):

    def __init__(self):
        super().__init__(message_type='resultRequest')
        self.job_id = ""

    def get_jobId(self):
        return self.job_id

    def set_jobId(self, job_id):
        self.job_id = job_id