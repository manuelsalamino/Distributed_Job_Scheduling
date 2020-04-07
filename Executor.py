import socket
import threading
import pickle
import time

ENCODING = 'utf-8'


class Executor(threading.Thread):


    """Commenti su possibili strategie e task da implementare

        - Gestione di un job inoltrato:
            # TODO quando l'Executor A ha inoltrato un suo job ad un altro Executor B e ad A arriva una ResultRequest dal
            #  Client che ha submittato tale job, invece di "inoltrare questa richiesta a B per sapere se il job è stato
            #  completato, aspettare che B risponda e ritornare tale risposta al Client", potremmo invece fare qualcosa di più
            #  intelligente: "A inoltra il job a B e nello stesso momento si salva lo stato di B come 'in esecuzione'; quando il
            #  client invia la ResultRequest ad A, A risponde direttamente con 'in esecuzione' senza inoltrare nessun messaggio
            #  a B; sarà invece B che quando avrà completato il job manda un messaggio ad A: quando A riceve quel messaggio,
            #  modifica lo stato del job inoltrato in modo che ad una prossima ResultRequest del Client, A possa rendere
            #  direttamente il risultato minimizzando il numero di messaggi [è il pattern Publish-Subscribe]"

        - Trovare un modo per salvare le informazioni in un file in modo da gestire la Fault Tolerance

        - Definire gli stati di un job: waiting, executing, completed ?   [waiting nel senso che è nella lista con gli
                                                                            altri processi, ma non è ancora in esecuzione]
    """

    """
    In sostanza ci sono 3 THREADS che devono essere eseguiti contemporaneamente:
        - Accettare le richieste del Client
        - Ricevere e processare (eventualmente inviare jobs) il token
        - Eseguire un job
        
        - Un quarto thread potrebbe gestire il salvataggio delle informazioni in un file per la Fault Tolerance: 
            credo si possa supporre che mentre questo salvataggio è in atto, il server non possa fallire
    """

    def __init__(self, my_host, my_port, id=0):
        threading.Thread.__init__(self, name='server_' + str(id))
        self.host = my_host
        self.port = my_port
        self.id = id
        self.jobs = []    # questa lista è brutta ma serve per avere un modo per accedere ai jobs per poter prendere i loro attributi (es execution_time)
                    # TODO ?? Forse è meglio che self.jobs sia un dizionario del tipo: {job_id: job} ? In questo modo
                    #  accediamo direttamente al job che ci interessa

        self.running_jobs = {}    # {'job_id': starting_time}  # TODO ?? l'Executor può eseguire un job alla volta no? Perché un dizionario?
        self.completed_jobs = {}       # {'job_id': result_value}
        self.job_counter = 0        # usato nella creazione del job_id
        self.forwarded_jobs = []    # per tenere traccia dei jobs inoltrati ad altri executor. Ogni elemento è: (job_id, "server_host+server_port")

    def check_complete_jobs(self):   # TODO ?? Non ho ben capito cosa fa questa funzione
        for job in self.jobs:     # uso la lista brutta
            if job.get_id() in self.running_jobs.keys() and time.time() - self.running_jobs[job.get_id()] > job.get_execution_time():
                job.set_status('complete')
                del self.running_jobs[job.get_id()]
                self.completed_jobs[job.get_id()] = job.get_final_result()

    def process_request(self, request, connection):
        request_type = request.get_type()
        print(f'request type : {request_type}')

        if request_type == "jobRequest":

            print('job Request arrived')
            job = request.requested_job

            # Set job_id
            job_id = self.getName() + str(self.job_counter)
            self.job_counter += 1
            job.set_id(job_id)  # give the id to the job

            job.set_sent_to(self.name)
            job.set_status('executing') # TODO oppure si mette waiting?
            self.jobs.append(job)
            self.running_jobs[job_id] = time.time()  # add to the running_jobs list the new job with the corresponding starting time
            message = str(job_id)  # the message is the job_id of the received job

        if request_type == "resultRequest":
            print('result Request arrived')

            self.check_complete_jobs()  # every time the client ask for a result, update the status of all the jobs

            if int(request.get_jobId()) in self.running_jobs.keys():
                message = "executing"
            else:
                message = 'The result is ' + str(self.completed_jobs[int(request.get_jobId())])

        connection.send(message.encode(ENCODING))

    def run(self):
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.bind((self.host, self.port))
        sock.listen(10)  # The argument specifies the number of unaccepted connections that the system will allow before refusing new connection

        while True:
            connection, client_address = sock.accept()
            print(f'Connected with Client {client_address[0]} : {client_address[1]}')

            print('Receiving data from client...')
            data = connection.recv(4096)  # Receiving message from client
            print('Message arrived')
            request = pickle.loads(data)

            self.process_request(request, connection)

            connection.close()
            # break






if __name__ == '__main__':
    my_host = input("which is my host? ")
    #my_host = '127.0.0.1'
    my_port = int(input("which is my port? "))
    #my_port = 41
    my_id = int(input("Which is my id?"))
    executor = Executor(my_host, my_port, my_id)
    executor.start()