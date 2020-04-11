from Request.Request import Request

class SendResult(Request):

    def __init__(self, job_id, result):
        super().__init__(message_type="sendResult")
        self.job_id = job_id
        self.result = result

    def getJobId_Result(self):
        return self.job_id, self.result