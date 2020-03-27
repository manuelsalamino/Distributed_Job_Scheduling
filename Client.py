import socket
import threading
import pickle
from Request.JobRequest import JobRequest
from Request.ResultRequest import ResultRequest

ENCODING = 'utf-8'

class Client(threading.Thread):

    def __init__(self, server_host, server_port, name='client'):
        threading.Thread.__init__(self, name=name)
        self.host = server_host
        self.port = server_port

        # TODO visto che sono threads, queste due variabili vanno messe condivise tra i thread ?
        self.jobs_submitted = [] # add the returned job_id to the list
        self.jobs_completed = [] # pop from jobs_submitted and add the completed job the this list

    def run(self):
        if not self.jobs_submitted:
            request = JobRequest()

        else:
            # TODO find a way to choose JobRequest or ResultRequest
            pass


        print('Sending message')
        message = pickle.dumps(request)

        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.connect((self.host, self.port)) # Connection to the server

        s.sendall(message)
        #s.shutdown(socket.SHUT_RDWR)
        print('Message sent')
        print('Waiting a response')
        self.received_data = s.recv(4096)  # Wait for the job_id or result
        print('Response arrived')
        s.close()

        if request.get_type() == 'jobRequest':
            self.jobs_submitted.append(self.received_data)

        if request.get_type() == 'resultRequest' and self.received_data is not None:
            index = self.jobs_submitted.index(self.received_data[0])
            self.jobs_completed.append(self.jobs_submitted.pop(index))

        print(self.jobs_submitted)




if __name__ == '__main__':
    server_host = input("what is server's host? ")
    server_port = int(input("what is server's port?"))
    sender = Client(server_host, server_port)
    sender.start()