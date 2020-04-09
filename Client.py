import socket
import threading
import pickle
import random
import time
from Job import Job
from Request.JobRequest import JobRequest
from Request.ResultRequest import ResultRequest

ENCODING = 'utf-8'


class Client(threading.Thread):

    def __init__(self, server_host, server_port, n_jobs, name='client'):
        threading.Thread.__init__(self, name=name)
        self.host = server_host
        self.port = server_port

        # TODO visto che sono threads, queste due variabili vanno messe condivise tra i thread ?
        self.jobs_submitted = []   # add the returned job_id to the list
        self.jobs_completed = {}   # pop from jobs_submitted and add to this list. Element: {'job_id': result}
        self.job_count = n_jobs

    def generate_request(self):
        if not self.jobs_submitted and self.job_count > 0:  # if it is the first action, it is a JobRequest
            job = Job()  # create the job
            request = JobRequest(job=job)
            print("request:     start new job")
            self.job_count -= 1

        elif self.job_count > 0:
            type_of_message = random.randint(0, 1)  # 0: send a new request; 1: check an already submitted request
            if type_of_message == 0:
                job = Job()     # create the job
                request = JobRequest(job=job)
                print("action:     start new job")
            else:
                tmp = self.jobs_submitted[random.randrange(len(self.jobs_submitted))] # choose randomly a request already submitted
                request = ResultRequest(tmp)
                print("action:     result for job", tmp)
        else:
            tmp = self.jobs_submitted[random.randrange(len(self.jobs_submitted))]  # choose randomly a request already submitted
            request = ResultRequest(tmp)
            print("action:     result for job", tmp)

        return request

    def run(self):

        n = self.job_count
        while len(self.jobs_completed) < n:
            s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            s.connect((self.host, self.port))  # Connection to the server

            request = self.generate_request()     # randomly create a request

            message = pickle.dumps(request)
            s.sendall(message)

            received_data = s.recv(4096)  # Wait for the job_id or result # TODO Fault Tolerance: l'Executor potrebbe guastarsi prima di mandare il job_id/result
            received_data = received_data.decode(ENCODING)
            s.close()

            if request.get_type() == 'jobRequest':
                self.jobs_submitted.append(received_data)

            if request.get_type() == 'resultRequest' and received_data is not None:
                if received_data == "waiting":
                    print("response:        the job is waiting")
                elif received_data == "executing":
                    print("response:        the job is executing")
                else:
                    index = self.jobs_submitted.index(request.get_jobId())
                    del self.jobs_submitted[index]
                    self.jobs_completed[request.get_jobId()] = received_data
                    print("response:      the result of the job is ", received_data)

            print("submitted jobs: ", self.jobs_submitted)
            print("completed jobs: ", self.jobs_completed)
            print("")
            time.sleep(2)




if __name__ == '__main__':
    server_host = input("what is server's host? ")
    #server_host = '127.0.0.1'
    server_port = int(input("what is server's port?"))
    #server_port = 41
    n_jobs = int(input("How many jobs?"))
    sender = Client(server_host, server_port, n_jobs)
    sender.start()