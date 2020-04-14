import socket
import threading
import pickle
import time
import random
import collections
import sys
import os
from Request.ForwardJob import ForwardJob
from Request.Token import Token
from Request.SendResult import SendResult

ENCODING = 'utf-8'


class Executor(object):


    """Commenti su possibili strategie e task da implementare

        x Gestione di un job inoltrato:
            # TODO quando l'Executor A ha inoltrato un suo job ad un altro Executor B e ad A arriva una ResultRequest dal
            #  Client che ha submittato tale job, invece di "inoltrare questa richiesta a B per sapere se il job è stato
            #  completato, aspettare che B risponda e ritornare tale risposta al Client", potremmo invece fare qualcosa di più
            #  intelligente: "A inoltra il job a B e nello stesso momento si salva lo stato di B come 'in esecuzione'; quando il
            #  client invia la ResultRequest ad A, A risponde direttamente con 'in esecuzione' senza inoltrare nessun messaggio
            #  a B; sarà invece B che quando avrà completato il job manda un messaggio ad A: quando A riceve quel messaggio,
            #  modifica lo stato del job inoltrato in modo che ad una prossima ResultRequest del Client, A possa rendere
            #  direttamente il risultato minimizzando il numero di messaggi [è il pattern Publish-Subscribe]"

        - Trovare un modo per salvare le informazioni in un file in modo da gestire la Fault Tolerance

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
        self.name = self.host + ':' + str(self.port)

        """
        - Controllo accesso risorse: dict e liste sono thread-safe in Python, mentre OrderedDict non lo è
        """

        self.waiting_jobs = collections.OrderedDict()      # {job_id : job}   I jobs verranno poppati una volta che inizia la loro esecuzione
        self.running_job = {}        # {job_id: job}    elemento singolo (può essere eseguito solo un job alla volta)
        self.completed_jobs = {}  # {'job_id': result_value}
        self.job_counter = 0        # usato nella creazione del job_id
        self.forwarded_jobs = {}    # per tenere traccia dei jobs inoltrati ad altri executor. Ogni elemento è: {job_id : "server_host+server_port")}

        # Token
        self.next_executor_host = next_ex_host
        self.next_executor_port = next_ex_port

        # Locks
        self.waiting_jobs_lock = threading.Lock()    # OrderedDict non è thread-safe # TODO regolare accesso a self.waiting_jobs

        # Save Executor State
        self.filename = 'executor' + str(self.id) + '.pkl'       # nome del file in cui viene salvato lo stato dell'oggetto

    def getName(self):
        return self.name

    def handle_jobRequest(self, request):
        print('\tjob Request arrived')
        job = request.requested_job

        # Set job_id
        job_id = self.getName() + '_' + str(self.job_counter)
        self.job_counter += 1
        job.set_id(job_id)  # give the id to the job

        job.set_sent_to(self.name)
        job.set_status('waiting')

        self.waiting_jobs[job_id] = job

        message = str(job_id)  # the message is the job_id of the received job

        return message

    def handle_resultRequest(self, request):
        print('\tresult Request arrived')

        job_id = request.get_jobId()
        # message = 'waiting'  # Ogni tanto mi dà questo problema: "UnboundLocalError: local variable 'message' referenced before assigment"
                             # Sembra che in certi momenti non entri in nessuno di quegli if, dunque non crea nessun 'message'.
                             # Probabilmente è per una questione di accesso multiplo di più thread ad una stessa variabile

        if job_id in self.waiting_jobs.keys():
            message = "waiting"
        elif job_id in self.running_job.keys():
            message = "executing"
        elif job_id in self.completed_jobs.keys():
            message = str(self.completed_jobs[job_id])
        elif job_id in self.forwarded_jobs.keys():
            message = "waiting"

        return message


    def handle_token(self, request):
        # TODO prima di inviare il messaggio forwardJob, fare un check per vedere se effettivamente c'é il numero di messaggi richiesti in self.waiting_jobs
        # TODO per gestire l'accesso in questo caso, forse è meglio mettere tutto handle_token in lock in process_request() ?

        print('\nToken received')

        print('Handle_token waiting for lock')
        self.waiting_jobs_lock.acquire()
        try:
            print('Handle_token acquired lock')
            # Update its attributes in the token
            request.update(self.host + ':' + str(self.port), self.id, len(self.waiting_jobs) + len(self.running_job))

            # Check if the server can forward some jobs
            forwarding_candidates = request.check_possible_forwarding(self.id)

            # Forward jobs
            if len(forwarding_candidates) > 0:
                for k, v in forwarding_candidates.items():
                    print(f'\tForwarding {v} jobs to {k}')
                    k = k.split(':')
                    address, port = k[0], k[1]

                    # Create ForwardJob message to pack the jobs and send the message
                    # TODO Thread-safe : gestire accesso a self.waiting_jobs
                    job_to_forward = {}
                    for i in range(v):
                        job_id, job = self.waiting_jobs.popitem(last=True)   # Prendiamo gli ultimi job aggiunti alla queue
                        self.forwarded_jobs[job_id] = 'waiting'         # Questo dizionario verrà aggiornato quando l'Executor, a cui sono stati inoltrati i jobs,
                                                                        # avrà finito uno dei job e manderà il risultato che verrà scritto direttamente qui [spiegato meglio sopra]
                        job_to_forward[job_id] = job

                    print(f'\tForwarding jobs: {job_to_forward.keys()}')
                    forwardJob = ForwardJob(job_to_forward)

                    message = pickle.dumps(forwardJob, protocol=pickle.HIGHEST_PROTOCOL)

                    forwarding_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                    forwarding_sock.connect((address, int(port)))

                    forwarding_sock.sendall(message)

                    # TODO Fault Tolerance:
                    #  aspettare per tot tempo un ACK: ci sarà una funzione di recv che ti permette di avere un timeout dopo il quale rinvii il messaggio.
                    #  Alternativa: Salvarsi i messaggi forwardJob in un dizionario {'ip:port dell'Executor a cui inoltriamo' : forwardJob }.
                    #  L'Executor che riceve questo messaggio forwardJob risponderà con un messaggio (ACK) con il proprio
                    #  'ip:port' in modo da poter andare  direttamente a cancellare il dizionario quando il job è stato ricevuto.

                    ack = forwarding_sock.recv(4096) # wait for the ACK
                    if ack == 'ok':
                        print('\tACK arrivato')

                    forwarding_sock.close()
            else:
                print('\tNetwork is balanced ')
        finally:
            self.waiting_jobs_lock.release()
            print('Handle_token releases lock')

        # forward the token to the next Executor
        time.sleep(3) # Ritardo nell'invio, giusto per provare
        token = pickle.dumps(request)
        token_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        token_sock.connect((self.next_executor_host, self.next_executor_port))
        token_sock.sendall(token)
        print(f'Token sent to {self.next_executor_host}:{self.next_executor_port}\n')
        token_sock.close()

    def handle_forwardedJob(self, dict_of_jobs):
        print(f'\tBefore receive jobs: {self.waiting_jobs.keys()}')
        for k, v in dict_of_jobs.items():
            self.waiting_jobs[k] = v

        print(f'\tAfter receive jobs: {self.waiting_jobs.keys()}')
        return 'ok' # ACK

    def handle_sendResult(self, request):
        """
        Send the result to the Executor that forwarded the job
        :param request:
        :return:
        """

        job_id, result = request.getJobId_Result()
        print(f'\tReceived result of forwarded job: {job_id} : {result}')
        self.completed_jobs[job_id] = result
        del self.forwarded_jobs[job_id]

    def process_request(self, request, connection):
        request_type = request.get_type()
        print(f'\trequest type : {request_type}')

        if request_type == "jobRequest":
            message = self.handle_jobRequest(request)
            connection.send(message.encode(ENCODING))
            #connection.close()

        if request_type == "resultRequest":
            message = self.handle_resultRequest(request)
            connection.send(message.encode(ENCODING))
            # connection.close()

        if request_type == 'token':
            connection.close()
            self.handle_token(request)

        if request_type == 'forwardJob':
            message = self.handle_forwardedJob(request.get_forwarding_job())
            connection.send(message.encode(ENCODING)) # ACK message
            connection.close()

        if request_type == 'sendResult':
            self.handle_sendResult(request)
            connection.close()

    def process_job(self):
        while True:
            if self.waiting_jobs:

                # Get the first job_id and load the corresponding job
                print('Process_job waiting for lock')
                self.waiting_jobs_lock.acquire()
                try:
                    print('Process_job acquire lock')
                    job_id, job = self.waiting_jobs.popitem(last=False)         # pop del primo elemento (il più "vecchio")
                finally:
                    self.waiting_jobs_lock.release()
                    print('Process_job releases lock')

                self.running_job[job_id] = job              # sposto il job
                print(f'\tExecuting job: {job_id}')          # print job_id
                job.set_status("executing")             # set new status

                # Execute the job
                time.sleep(job.get_execution_time())

                # job execution complete
                del self.running_job[job_id]          # delete job from dict
                result = job.get_final_result()
                self.completed_jobs[job_id] = result          # add result to dict

                # If the job was forwarded, send result message to the sender of the job
                sender = job_id.split('_')[0]
                if sender != self.name:
                    addr, port = sender.split(':')
                    send_res = SendResult(job_id, result)
                    res_message = pickle.dumps(send_res)

                    sendRes_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                    sendRes_sock.connect((addr, int(port)))

                    sendRes_sock.sendall(res_message)
                    print(f'Forwarded job {job_id} completed --> Result : {result}')

                print(f'\tJob: {job_id} completed --> Result : {result}')

            else:
                time.sleep(2)       # altrimenti fa controlli a vuoto (per non sovraccaricare il pc)
                # TODO capire come fare la terminazione

    def save_state(self):
        state = self.__dict__.copy()
        del state['waiting_jobs_lock']    # thread.lock element non può essere fatto il pickle, quindi lo eliminiamo

        with open(self.filename, 'wb') as f:
            pickle.dump(state, f, protocol=pickle.HIGHEST_PROTOCOL)

    def restore_state(self):
        with open(self.filename, 'rb') as f:  # TODO aggiungere self.waiting_jobs_lock quando si fa pickle.load
            state = pickle.load(f)
        self.__dict__.update(state)          # restore dell'oggetto con tutti i dati che erano stati salvati
        self.waiting_jobs_lock = threading.Lock()           # aggiungo l'attributo lock di cui non potevo dare pickle
        print("Restore done!\n")

    def check_failure(self):
        if fail.is_set():
            print("Executor", self.name, "FAIL")
            sys.exit()           # simulo il fallimento

    def run(self):
        if os.path.isfile(self.filename):           # if a backup is available, use it
            self.restore_state()
        else:                                    # otherwise create it
            self.save_state()       # save the state

        # thread for jobs execution
        worker = threading.Thread(target=self.process_job, daemon=True)      # settato come Deamon così quando il main_thread viene stoppato and worker si stoppa
        worker.start()

        #time.sleep(1.1)     # TODO wait in modo da avere un fail adesso (per fare test)
        self.check_failure()        # check failure before create a socket for communication

        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.bind((self.host, self.port))
        sock.listen(10)  # The argument specifies the number of unaccepted connections that the system will allow before refusing new connection

        if self.id == 0:
            token = Token(3)    # TODO aggiornare dinamicamente
            time.sleep(30)      # TODO spostare questo
            self.handle_token(token)

        while True:
            connection, client_address = sock.accept()
            print(f'Connected with Client {client_address[0]} : {client_address[1]}')

            time.sleep(1.1)     # TODO wait in modo da avere un fail adesso (per fare test)
            self.check_failure()             # check failure before receive request

            #print('Receiving data from client...')
            data = connection.recv(4096)  # Receiving message from client
            #print('Message arrived')
            request = pickle.loads(data)
            #print(request)

            thread = threading.Thread(target=self.process_request, args=(request, connection))
            thread.start()
            thread.join()

            print(f'\n\trunning: {self.running_job.keys()}\n\twaiting: {self.waiting_jobs.keys()}\n')


            # break


def executor_fail(f):
    f.set()             # set to True flag di Event, significa che c'è stato un fail


if __name__ == '__main__':
    # #my_host = input("which is my host? ")
    # my_host = '127.0.0.1'
    # my_port = int(input("which is my port? "))
    # #my_port = 8881
    # my_id = int(input("Which is my id?"))
    # #my_id = 0
    # next_executor_ip = input("Next Executor IP?")
    # next_executor_port = int(input("Next Executor port?"))
    # executor = Executor(my_host=my_host, my_port=my_port, id=my_id, next_ex_host=next_executor_ip, next_ex_port=next_executor_port)
    # executor.run()

    # Create the network

    my_host = '127.0.0.1'
    my_id = int(input("Which is my id?"))

    if my_id == 0:
        executor = Executor(my_host=my_host, my_port=8881, id=my_id, next_ex_host=my_host,
                            next_ex_port=8882)

    if my_id == 1:
        executor = Executor(my_host=my_host, my_port=8882, id=my_id, next_ex_host=my_host,
                            next_ex_port=8883)

    if my_id == 2:
        executor = Executor(my_host=my_host, my_port=8883, id=my_id, next_ex_host=my_host,
                            next_ex_port=8881)

    if os.path.isfile('executor' + str(my_id) + '.pkl'):            # remove backup of old execution
        os.remove('executor' + str(my_id) + '.pkl')

    fail = threading.Event()  # event shared with the master thread (notify when a fail occurs)

    while True:
        threading.Timer(random.randint(0, 1), function=executor_fail, args=(fail,)).start()     # dopo un intervallo di tempo random esegue la funzione che dice che c'è stato un fail nell'executor

        exec = threading.Thread(target=executor.run)                # parte l'executor (o per la prima volta o dopo un fail)
        exec.start()

        exec.join()            # aspetto che avvenga un fallimento

        fail.clear()        # Event flag set to False


