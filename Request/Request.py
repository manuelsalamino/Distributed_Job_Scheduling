import time


class Request(object):

    def __init__(self, message_type="", sender_host="", sender_port=""):
        self.request_id = time.time()
        self.type = message_type
        self.sender_host = sender_host
        self.sender_port = sender_port

    def get_type(self):
        return self.type