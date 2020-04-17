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

    def generate_job_request(self):
        job = Job()  # create the job
        request = JobRequest(job=job)
        job.set_request_id(request.get_id())  # set the request_id in the job (needed to identify postponed request)
        print("request:     start new job")
        self.job_count -= 1

        return request

    def generate_result_request(self):
        tmp = self.jobs_submitted[random.randrange(len(self.jobs_submitted))]  # choose randomly a request already submitted
        request = ResultRequest(tmp)
        print("action:     result for job", tmp)

        return request

    def generate_request(self):
        if not self.jobs_submitted and self.job_count > 0:  # if it is the first action, it is a JobRequest
            request = self.generate_job_request()

        elif self.job_count > 0:
            type_of_message = random.randint(0, 1)  # 0: send a new request; 1: check an already submitted request
            if type_of_message == 0:
                request = self.generate_job_request()
            else:
                request = self.generate_result_request()
        else:
            request = self.generate_result_request()

        return request

    def run(self):

        n = self.job_count
        while len(self.jobs_completed) < n:

            request = self.generate_request()  # randomly create a request

            received_data = ''
            while len(received_data) == 0:      # se len(received_data) > 0 allora la connessione è andata a buon fine
                try:
                    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                    s.connect((self.host, self.port))  # Connection to the server

                    print(f'Connected with Executor {self.host}:{self.port}')

                    message = pickle.dumps(request)
                    s.sendall(message)
                    s.settimeout(5)     # if the socket does not recv data in that time, assume the Executor has crashed

                    received_data = s.recv(4096)  # Wait for the job_id or result # TODO Fault Tolerance: l'Executor potrebbe guastarsi prima di mandare il job_id/result
                    received_data = received_data.decode(ENCODING)

                    if len(received_data) == 0:       # a volte riceve dati vuoti ('')
                        raise ConnectionError

                    s.close()
                except (ConnectionError, socket.timeout):
                    # s.close()            # probabilmente non è necessario perchè la connessione è già chiusa
                    print(f'Connection problem with the Executor {self.host}:{self.port}\n\nReconnection...\n')
                    request.set_postponed()
                    time.sleep(2)           # riprova la connessione tra qualche secondo

            if request.get_type() == 'jobRequest':
                self.jobs_submitted.append(received_data)

            if request.get_type() == 'resultRequest':
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
            time.sleep(4)



if __name__ == '__main__':
    #server_host = input("what is server's host? ")
    server_host = '127.0.0.1'
    #server_port = int(input("what is server's port?"))
    server_port = 8882
    #n_jobs = int(input("How many jobs?"))
    n_jobs = 5

    sender = Client(server_host, server_port, n_jobs)
    sender.start()
