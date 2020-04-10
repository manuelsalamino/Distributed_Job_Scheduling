import socket
import threading
import pickle
import time
import collections
from Request.ForwardJob import ForwardJob
from Request.Token import Token

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

        - Un problema può essere che il token viaggi più velocemente dell'invio dei messaggi di forwardJob, forse dovremmo
            aspettare un po' prima di far partire il token al prossimo executor.

        - Fault Tolerance: probabilmente dobbiamo mettere un try: except: ogni volta che mandiamo un messaggio

        - La comunicazione è affidabile, ma cosa vuol dire? Dobbiamo assumere che qualsiasi messaggio inviato arrivi a destinazione?
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
        """

        :param my_host: server ip
        :param my_port: server port
        :param id:  server id
        :param next_ex_host: Executor ip a cui è in comunicazione. La struttura è un Ring, ogni server sarà connesso solo con il successivo server
        :param next_ex_port:  Executor port a cui è in comunicazione
        """
        # Server
        self.host = my_host
        self.port = my_port
        self.id = id
        self.name = self.host + ':' + str(self.port) + '_' + str(id)

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
        # TODO gestire l'accesso dei vari thread a self.waiting_jobs.
        # TODO prima di inviare il messaggio forwardJob, fare un check per vedere se effettivamente c'é il numero di messaggi richiesti in self.waiting_jobs
        print('Token arrived')

        # Update its attributes in the token
        request.update(self.host + ':' + str(self.port), self.id, len(self.waiting_jobs))

        # Check if the server can forward some jobs
        forwarding_candidates = request.check_possible_forwarding(self.id)

        # Forward jobs
        if len(forwarding_candidates) > 0:
            for k, v in forwarding_candidates.items():
                print(f'Forwarding {v} jobs to {k}')
                k = k.split(':')
                address, port = k[0], k[1]


                print(f'')
                # Create ForwardJob message to pack the jobs and send the message
                job_to_forward = {}
                for i in range(v):
                    job_id, job = self.waiting_jobs.popitem(last=True)   # Prendiamo gli ultimi job aggiunti alla queue
                    # TODO Fault Tolerance: aggiugere i job a questo dizionario solo quando si è ricevuto l'ack
                    self.forwarded_jobs[job_id] = 'waiting'         # Questo dizionario verrà aggiornato quando l'Executor, a cui sono stati inoltrati i jobs,
                                                                    # avrà finito uno dei job e manderà il risultato che verrà scritto direttamente qui [spiegato meglio sopra]
                    job_to_forward[job_id] = job

                forwardJob = ForwardJob(job_to_forward)

                message = pickle.dumps(forwardJob, protocol=pickle.HIGHEST_PROTOCOL)

                forwarding_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                forwarding_sock.connect((address, int(port)))

                forwarding_sock.sendall(message)

                # TODO Fault Tolerance: aspettare per tot tempo un ACK: ci sarà una funzione di recv che ti permette di
                #  avere un timeout dopo il quale rinvii il messaggio.
                #  Alternativa: Salvarsi i messaggi forwardJob in un dizionario {'ip:port dell'Executor a cui inoltriamo' : forwardJob }.
                #  L'Executor che riceve questo messaggio forwardJob risponderà con un messaggio (ACK) con il proprio
                #  'ip:port' in modo da poter andare  direttamente a cancellare il dizionario quando il job è stato ricevuto.
                ack = forwarding_sock.recv(4096) # wait for the ACK
                if ack == 'ok':
                    print('ACK arrivato')

                forwarding_sock.close()
        else:
            print('Network is balanced ')

        # forward the token to the next Executor
        time.sleep(3) # Ritardo nell'invio, giusto per provare
        token = pickle.dumps(request)
        token_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        token_sock.connect((self.next_executor_host, self.next_executor_port))
        token_sock.sendall(token)
        token_sock.close()

    def handle_forwardedJob(self, dict_of_jobs):
        for k, v in dict_of_jobs.items():
            self.waiting_jobs[k] = v

        return 'ok' # ACK

        # TODO Quando questi jobs sono completati, mandare il risultato al server da cui provenivano [specificato nel job_id]

    def process_request(self, request, connection):
        request_type = request.get_type()
        print(f'request type : {request_type}')

        if request_type == "jobRequest":
            message = self.handle_jobRequest(request)
            connection.send(message.encode(ENCODING))
            #connection.close()

        if request_type == "resultRequest":
            message = self.handle_resultRequest(request)
            connection.send(message.encode(ENCODING))
            # connection.close()

        if request_type == 'token':
            # TODO probabilmente qui la connessione dobbiamo chiuderla perché non ci interessa mandare messaggi a quello che ci ha inviato il token
            connection.close()
            self.handle_token(request)

        if request_type == 'forwardJob':
            message = self.handle_forwardedJob(request.get_forwarding_job())
            # connection.send(message.encode(ENCODING)) # ACK message
            connection.close()

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
                time.sleep(2)       # altrimenti da controlli a vuoto (per non sovraccaricare il pc)
                # TODO capire come fare la terminazione


    def run(self):

        # TODO se l'id del server è == 0, crea l'oggetto Token, aspetta un po e fallo partire

        worker = threading.Thread(target=self.process_job)
        worker.start()

        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.bind((self.host, self.port))
        sock.listen(10)  # The argument specifies the number of unaccepted connections that the system will allow before refusing new connection

        if self.id == 0:
            token = Token(2)
            time.sleep(60)      # TODO spostare questo
            self.handle_token(token)

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
    my_host = input("which is my host? ")
    #my_host = '127.0.0.1'
    my_port = int(input("which is my port? "))
    #my_port = 8881
    my_id = int(input("Which is my id?"))
    #my_id = 0
    next_executor_ip = input("Next Executor IP?")
    next_executor_port = int(input("Next Executor port?"))
    executor = Executor(my_host=my_host, my_port=my_port, id=my_id, next_ex_host=next_executor_ip, next_ex_port=next_executor_port)
    executor.run()
