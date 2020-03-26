import time

class Request():

    def __init__(self, type=""):
        self.request_id = time.time()
        self.type = type

    def get_type(self):
        return self.type