import socket
import threading
import pickle
import time

ENCODING = 'utf-8'


class Executor(threading.Thread):

    job_id = 0   # static attribute to autoincrement the job_id

    def __init__(self, my_host, my_port):
        threading.Thread.__init__(self, name='server')
        self.host = my_host
        self.port = my_port
        self.jobs = []    # questa lista è brutta ma serve per avere un modo per accedere ai jobs per poter prendere i loro attributi (es execution_time)
        self.running_jobs = {}    # {'job_id': starting_time}
        self.completed_jobs = {}       # {'job_id': result_value}
        # TODO self.storage_ip = ''

    def check_complete_jobs(self):
        for job in self.jobs:     # uso la lista brutta
            if job.get_id() in self.running_jobs.keys() and time.time() - self.running_jobs[job.get_id()] > job.get_execution_time():
                job.set_status('complete')
                del self.running_jobs[job.get_id()]
                self.completed_jobs[job.get_id()] = job.get_final_result()

    def run(self):
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.bind((self.host, self.port))
        sock.listen(10)  # The argument specifies the number of unaccepted connections that the system will allow before refusing new connection

        while True:
            connection, client_address = sock.accept()
            print('Connected with a Client')
            # #while True:
            #     print('Receiving data from client')
            #     data = connection.recv(4096) # Receiving message from client
            #     print(('Continue to receive data'))
            #     if not data:
            #         # Once client sent data, we need to distinguish between messages (submit a job or retrieve the result)
            #         print('Message arrived')
            #         self.request = pickle.loads(data)
            #         request_type = self.request.get_type()
            #         print(f'request type : {request_type}')
            #
            #         if request_type == "jobRequest":
            #             # TODO return job_id to the client
            #             print('job Request arrived')
            #             message = 'job_id: 1234'
            #             sock.send(message.encode(ENCODING))
            #
            #         if request_type == "resultRequest":
            #             pass # Do something
            #
            #         #break
            # connection.close()

            print('Receiving data from client...')
            data = connection.recv(4096)  # Receiving message from client
            print('Message arrived')
            request = pickle.loads(data)
            request_type = request.get_type()
            print(f'request type : {request_type}')

            if request_type == "jobRequest":
                # TODO add the job to the storage, retrieve the job_id (index of the dataframe) and return the job_id to the client
                print('job Request arrived')
                job = request.requested_job
                job_id = Executor.job_id
                job.set_id(job_id)   # give the id to the job
                job.set_status('executing')
                self.jobs.append(job)
                self.running_jobs[job_id] = time.time()  # add to the running_jobs list the new job with the corresponding starting time
                message = str(job_id)   # the message is the job_id of the received job

                Executor.job_id = Executor.job_id + 1   # increment the static attribute for the next job

            if request_type == "resultRequest":
                print('result Request arrived')

                self.check_complete_jobs()   # every time the client ask for a result, update the status of all the jobs

                if int(request.get_jobId()) in self.running_jobs.keys():
                    message = "executing"
                else:
                    message = 'The result is ' + str(self.completed_jobs[int(request.get_jobId())])

            connection.send(message.encode(ENCODING))
            connection.close()







if __name__ == '__main__':
    #my_host = input("which is my host? ")
    my_host = '127.0.0.1'
    #my_port = int(input("which is my port? "))
    my_port = 41
    executor = Executor(my_host, my_port)
    executor.start()