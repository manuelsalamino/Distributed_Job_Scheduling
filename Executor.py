import socket
import threading
from Request import Request, ResultRequest, JobRequest
import pickle

ENCODING = 'utf-8'

class Executor(threading.Thread):

    def __init__(self, my_host, my_port):
        threading.Thread.__init__(self, name=my_host+str(my_port))
        self.host = my_host
        self.port = my_port


    def run(self):
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.bind((self.host, self.port))
        sock.listen(10)  # The argument specifies the number of unaccepted connections that the system will allow before refusing new connections
        connection, client_address = sock.accept()
        try:
            data = None
            while True:
                data = connection.recv(4096) # Receiving message from client
                if not data:
                    # Once client sent data, we need to distinguish between messages (submit a job or retrieve the result)

                    self.request = pickle.loads(data)
                    request_type = self.request.get_type()
                    if request_type == "jobRequest":
                        # TODO return job_id to the client
                        pass # Do something

                    if request_type == "resultRequest":
                        # TODO looking for the result: return a not_completed message if job is not completed yet, otherwise return the result
                        pass # Do something

                    break

        finally:
            connection.close()