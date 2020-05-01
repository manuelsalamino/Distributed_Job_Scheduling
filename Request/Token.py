import pandas as pd
pd.options.mode.chained_assignment = None     # avoid showing warning

from Request.Request import Request
import random
import numpy as np


class Token(Request):

    def __init__(self):
        """
        Each Executor updates the row with index == executor_id.
        Moreover, when the token arrives the first time to a server, it writes its 'host'+'port' on the corresponding 'host' cell in df
        """
        super().__init__(message_type='token')
        self.df = pd.DataFrame(columns=['host', 'n_jobs', 'inc'])

    def update(self, host_ip_port ,host_id, current_jobs):
        if host_id not in self.df.index:
            new_row = pd.Series({'host': host_ip_port, 'n_jobs':current_jobs, 'inc': 0})
            new_row.name = host_id
            self.df = self.df.append(new_row)

        self.df.loc[host_id, 'n_jobs'] = current_jobs
        self.df['inc'] = [0] * self.df.shape[0]
        print(self.df)
        print("------------------------------------------------")

    def check_possible_forwarding(self, host_id):
        """
        Per guardare a quale Executor mandare N jobs, si calcola la media definita dai valori salvati nel token, per ogni valore
        della colonna n_jobs si sottrae la media:
                            n_jobs - mean
        Considerando l'Executor che ha attualmente il token, si hanno 2 possibilità :
            -  n_jobs - mean <= 0 : L'Executor è bilanciato (= 0, ovvero ha un numero di jobs = alla media dei jobs) o sbilanciato in negativo
                ( < 0, dunque gli servirebbero dei jobs per averne un numero più vicino alla media ). In questo caso l'Executor non inoltra jobs
            - n_jobs - mean > 0: l'Executor è sbilanciato in positivo, dunque deve/può dare dei jobs agli altri Executor che sono sbilanciati
                in negativo
        :param host_id:
        :return:
        """
        num_jobs_to_forward = {}   # {'ip:port' : n_job_to_be_forwarded} Indica l'address del server a cui mandare tot jobs
        curr_df = self.df[~self.df['n_jobs'].isna()]
        if curr_df.shape[0] > 1:
            mean = int(np.ceil(np.mean(curr_df['n_jobs'].values)))
            curr_df['residual'] = curr_df['n_jobs'].apply(lambda x: x - mean)

            res = curr_df.loc[host_id, 'residual']
            if res <= 0:
                print(self.df)
                return {}

            #print("mean ", mean, ",    n_hosts ", len(curr_df['host']))
            tot_jobs_ceil = mean*len(curr_df['host'])
            #print("total ", tot_jobs_ceil)
            tot_jobs = np.sum(curr_df['n_jobs'])
            #print("tot_jobs ", tot_jobs)

            curr_df = curr_df.drop(host_id) # Drop the row corresponding to itself
            curr_df = curr_df[curr_df['residual'] < 0].sort_values(by='residual', ascending=True)

            if tot_jobs != tot_jobs_ceil:
                extra = tot_jobs_ceil - tot_jobs
                #print("jobs extra:", extra)
                for i in range(extra):
                    curr_df['residual'].iloc[i] += 1

            for idx, h, r in zip(curr_df.index, curr_df.host, curr_df.residual):
                if res + r <= 0:                # è res + r e non res - r perché r è già negativo
                    self.df.loc[idx, 'inc'] = res
                    self.df.loc[idx,'n_jobs'] += res
                    num_jobs_to_forward[h] = res
                    self.df.loc[host_id, 'n_jobs'] -= res
                    res = 0

                elif r < 0 and res + r > 0:
                    self.df.loc[idx, 'inc'] =  -r
                    self.df.loc[idx, 'n_jobs'] -= r
                    num_jobs_to_forward[h] = -r
                    self.df.loc[host_id, 'n_jobs'] += r
                    res += r

                if res == 0:
                    break

        print(self.df)
        return num_jobs_to_forward








