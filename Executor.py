import socket
import threading
import pickle
import time
import collections

ENCODING = 'utf-8'


class Executor(object):


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

        - Uno dei server deve far partire il token: per esempio, il server 0 lo fa sempre partire

        - Ci sarebbe la possibilità di mettere, per esempio, il thread che aspetta il Token (oppure quello che esegue i jobs)
            come un daemon invece che come un classico thread
    """

    """
    In sostanza ci sono 3 THREADS che devono essere eseguiti contemporaneamente:
        - Accettare le richieste del Client
        - Ricevere e processare (eventualmente inviare jobs) il token
        - Eseguire un job
        
        - Un quarto thread potrebbe gestire il salvataggio delle informazioni in un file per la Fault Tolerance: 
            credo si possa supporre che mentre questo salvataggio è in atto, il server non possa fallire
            
    Forse per creare più thread basta soltanto fare in modo che i messaggi dal Client arrivino in una certa porta, mentre quelli
    del token arrivino in un'altra porta ? 
    """

    def __init__(self, my_host, my_port, id=0, next_ex_host='localhost', next_ex_port=8000):
        #threading.Thread.__init__(self, name='server_' + str(id))
        # Server
        self.host = my_host
        self.port = my_port
        self.id = id
        self.name = 'server_' + str(id)

        # Jobs # TODO probabilmente dobbiamo gestire l'accesso (sincronizzazione) da parte di più thread ad una stessa risorsa

        # TODO non si può usare un dizionario per i jobs perché poi non si può fare il pop e simulare una coda di accesso
        self.waiting_jobs = collections.OrderedDict()      # {job_id : job}   I jobs verranno poppati una volta che inizia la loro esecuzione
        self.running_job = {}        # {job_id: job}    elemento singolo (può essere eseguito solo un job alla volta)
        self.completed_jobs = {}  # {'job_id': result_value}
        self.job_counter = 0        # usato nella creazione del job_id
        self.forwarded_jobs = {}    # per tenere traccia dei jobs inoltrati ad altri executor. Ogni elemento è: {job_id : "server_host+server_port")}

        # Token
        self.next_executor_host = next_ex_host
        self.next_executor_port = next_ex_port

    def getName(self):
        return self.name

    def handle_jobRequest(self, request):
        print('job Request arrived')
        job = request.requested_job

        # Set job_id
        job_id = self.getName() + str(self.job_counter)
        self.job_counter += 1
        job.set_id(job_id)  # give the id to the job

        job.set_sent_to(self.name)
        job.set_status('waiting')

        self.waiting_jobs[job_id] = job
        message = str(job_id)  # the message is the job_id of the received job

        return message

    def handle_resultRequest(self, request):
        print('result Request arrived')

        job_id = request.get_jobId()
        # job = {**self.jobs, **self.completed_jobs}[]

        if job_id in self.waiting_jobs.keys():
            message = "waiting"
        elif job_id in self.running_job.keys():
            message = "executing"
        elif job_id in self.completed_jobs.keys():
            message = str(self.completed_jobs[job_id])

        return message

    def handle_token(self, request):
        pass

    def process_request(self, request, connection):
        request_type = request.get_type()
        print(f'request type : {request_type}')

        if request_type == "jobRequest":
            message = self.handle_jobRequest(request)

        if request_type == "resultRequest":
            message = self.handle_resultRequest(request)

        if request_type == 'token':
            self.handle_token(request)

        connection.send(message.encode(ENCODING))

    def process_job(self):
        while True:
            if self.waiting_jobs:
                # Get the first job_id and load the corresponding job
                job_id, job = self.waiting_jobs.popitem(last=False)         # pop del primo elemento (il più "vecchio")
                self.running_job[job_id] = job              # sposto il job
                print(f'Executing job: {job_id}')          # print job_id
                job.set_status("executing")             # set new status

                # Execute the job
                time.sleep(job.get_execution_time())

                # job execution complete
                del self.running_job[job_id]          # delete job from dict
                self.completed_jobs[job_id] = job.get_final_result()          # add result to dict
                print(f'Job: {job_id} completed')

            else:
                time.sleep(1)       # altrimenti da controlli a vuoto (per non sovraccaricare il pc)
                # TODO capire come fare la terminazione



    def run(self):

        worker = threading.Thread(target=self.process_job)
        worker.start()

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
            print(request)

            thread = threading.Thread(target=self.process_request, args=(request, connection))
            thread.start()

            #print(threading.enumerate())
            # thread.join()


            # break


if __name__ == '__main__':
    #my_host = input("which is my host? ")
    my_host = '127.0.0.1'
    #my_port = int(input("which is my port? "))
    my_port = 41
    #my_id = int(input("Which is my id?"))
    my_id = 0
    executor = Executor(my_host, my_port, my_id)
    executor.run()
