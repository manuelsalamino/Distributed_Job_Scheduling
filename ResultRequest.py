import Request.Request

class ResultRequest(Request):

    def __init__(self):
        Request.__init__(self, type="resultRequest")
        self.job_id = ""

    def get_jobId(self):
        return self.job_id