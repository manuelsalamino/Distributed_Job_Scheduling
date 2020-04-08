import pandas as pd
from Request.Request import Request
import random


class Token(Request):

    def __init__(self, n_executors):
        """
        Each Executor updates the row with index == executor_id.
        Moreover, when the token arrives the first time to a server, it writes its 'host'+'port' on the corresponding 'host' cell in df
        :param n_executors: number of executor in the cluster

        """
        super().__init__(message_type='token')
        self.df = pd.DataFrame(index=list(range(n_executors)), columns=['host', 'n_jobs', 'inc']) # 'inc' probabilmente non serve a nulla
        self.df = self.df[[ 'n_jobs', 'inc']].fillna(0)
        self.df = self.df['host'].fillna('')

    def update(self, host_id, current_jobs):
        self.df.loc[host_id, 'n_jobs'] = current_jobs

    def forward_job(self, from_host_id, to_host_id):
        pass
