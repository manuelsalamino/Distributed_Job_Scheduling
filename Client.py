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

    def __init__(self, server_host, server_port, name='client'):
        threading.Thread.__init__(self, name=name)
        self.host = server_host
        self.port = server_port

        # TODO visto che sono threads, queste due variabili vanno messe condivise tra i thread ?
        self.jobs_submitted = []   # add the returned job_id to the list
        self.jobs_completed = []   # pop from jobs_submitted and add the completed job the this list. Ogni elemento sarà: (job_id, result)

    def generate_request(self):
        if not self.jobs_submitted:  # if it is the first action, it is a JobRequest
            job = Job()  # create the job
            request = JobRequest(job=job)
            print("request:     start new job")

        else:
            type_of_message = random.randint(0, 1)  # 0: send a new request; 1: check an already submitted request
            if type_of_message == 0:
                job = Job()     # create the job
                request = JobRequest(job=job)
                print("action:     start new job")
            else:
                tmp = self.jobs_submitted[random.randrange(len(self.jobs_submitted))] # choose randomly a request already submitted
                request = ResultRequest(tmp)
                print("action:     result for job", tmp)

        return request

    def run(self):

        for i in range(10):
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
                if received_data == "executing":
                    print("response:        the job is executing")
                else:
                    index = self.jobs_submitted.index(request.get_jobId())
                    self.jobs_completed.append((self.jobs_submitted.pop(index), received_data))
                    print("response:        ", received_data)

            print("submitted jobs: ", self.jobs_submitted)
            print("completed jobs: ", self.jobs_completed)
            print("")
            time.sleep(2)




if __name__ == '__main__':
    server_host = input("what is server's host? ")
    #server_host = '127.0.0.1'
    server_port = int(input("what is server's port?"))
    #server_port = 41
    sender = Client(server_host, server_port)
    sender.start()