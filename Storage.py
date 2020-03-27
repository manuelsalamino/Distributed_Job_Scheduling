import pandas as pd


class Storage(object):

    # TODO visto che lo Storage è condiviso tra tutti gli executor, il job_id potrebbe essere l'indice del dataframe

    def __init__(self):
        self.status = pd.DataFrame(columns=['ip_client', 'ip_server', 'ip_executor', 'result'])

    def add_status(self, ip_client, ip_server, ip_executor):
        self.status.append({'ip_client': ip_client, 'ip_server': ip_server, 'ip_executor': ip_executor, 'result': -1},
                           ignore_index=True)

    def get_jobId_status(self, job_id):
        return self.status['result'][job_id]
