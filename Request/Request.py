import time


class Request(object):

    def __init__(self, message_type="", sender_host="", sender_port=""):
        self.request_id = time.time()
        self.postponed = False
        self.type = message_type
        self.sender_host = sender_host
        self.sender_port = sender_port

    def get_id(self):
        return self.request_id

    def set_postponed(self):
        self.postponed = True

    def get_postponed(self):
        return self.postponed

    def get_type(self):
        return self.type