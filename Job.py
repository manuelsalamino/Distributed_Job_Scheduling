import random


class Job:
    def __init__(self, name=""):
        self.name = name
        self.job_id = None
        self.execution_time = random.randrange(1, 30)
        self.status = 'ready'
        self.final_result = None
        self.sent_to = ''
        self.executed_by = ''

    def set_id(self, id):
        self.job_id = id

    def get_id(self):
        return self.job_id

    def get_execution_time(self):
        return self.execution_time

    def set_status(self, stat):
        self.status = stat

    def get_status(self):
        return self.status

    def get_final_result(self):
        if self.final_result is None:   # if the result was not computed yet, compute it (simulation)
            self.final_result = random.randint(0, 100)
        return self.final_result

    def set_sent_to(self, executor):
        self.sent_to = executor

    def get_sent_to(self):
        return self.sent_to

    def set_executed_by(self, executor):
        self.executed_by = executor

    def get_executed_by(self):
        return self.executed_by
