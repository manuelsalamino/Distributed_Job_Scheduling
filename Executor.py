import socket
import threading
import pickle

ENCODING = 'utf-8'

class Executor(threading.Thread):

    def __init__(self, my_host, my_port):
        threading.Thread.__init__(self, name='server')
        self.host = my_host
        self.port = my_port
        # TODO self.storage_ip = ''


    def run(self):
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.bind((self.host, self.port))
        sock.listen(10)  # The argument specifies the number of unaccepted connections that the system will allow before refusing new connections
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

        print('Receiving data from client')
        data = connection.recv(4096) # Receiving message from client
        print('Message arrived')
        self.request = pickle.loads(data)
        request_type = self.request.get_type()
        print(f'request type : {request_type}')

        if request_type == "jobRequest":
            # TODO add the job to the storage, retrieve the job_id (index of the dataframe) and return the job_id to the client
            print('job Request arrived')
            message = 'job_id: 1234'
            connection.send(message.encode(ENCODING))

        if request_type == "resultRequest":
            # TODO check if the job is done or not, return the status/result of the job
            pass # Do something



if __name__ == '__main__':
    my_host = input("which is my host? ")
    my_port = int(input("which is my port? "))
    executor = Executor(my_host, my_port)
    executor.start()