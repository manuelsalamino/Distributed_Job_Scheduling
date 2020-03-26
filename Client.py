import socket
import threading
import pickle
from Request import ResultRequest, JobRequest

ENCODING = 'utf-8'

class Client(threading.Thread):

    def __init__(self, server_host, server_port):
        threading.Thread.__init__(self, name="messenger_sender")
        self.host = server_host
        self.port = server_port

        # TODO visto che sono threads, queste due variabili vanno messe condivise tra i thread ?
        self.jobs_submitted = [] # add the returned job_id to the list
        self.jobs_completed = [] # pop from jobs_submitted and add the completed job the this list

    def run(self):
        # Send jobRequest or resultRequest
        request = None # TODO assign class jobRequest or resultRequest
        while True:
            # Distinguere i due casi di Request:

            message = pickle.dumps(request)

            s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            s.connect((self.host, self.port)) # Connection to the server

            s.sendall(message)
            #s.shutdown(socket.SHUT_RDWR)
            self.received_data = s.recv(4096)  # Wait for the job_id or result
            s.close()

            """
            Pseudo-code:
            if message_sent is jobRequest:
                self.jobs_submitted.append(self.received_data)
            
            else if message_sent is resultRequest:
                # Il data ricevuto è il risultato di un job richiesto, dunque questa risposta potrebbe essere codificata come
                # una tupla (jobs_id, result)
                index = self.jobs_submitted.index(self.received_data[0])
                self.jobs_completed.append(self.jobs_submitted.pop(index))
            
            """


def main():
    server_host = input("what is server's host? ")
    server_port = int(input("what is server's port?"))
    sender = Client(server_host, server_port)



if __name__ == '__main__':
    main()