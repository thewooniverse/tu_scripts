import os
import pandas as pd
import numpy as np


csv_path = f"{os.getcwd()}{os.path.sep}CSVs{os.path.sep}distal_real.csv"
df = pd.read_csv(csv_path)

columns = ['time', 'type', 'username', 'userid', 'deviceid', 'istransaction',
          'gameid', 'gametype', 'level', 'isbot', 'reason', 'result', 'endstatus', 'opponentusername',
          'prevbalance','newbalance', 'prevescrow', 'newescrow', 'balancechange', 'escrowchange',
          'originalprizetype', 'awardedprizetype', 'awardedprizeamount',
          'usedpowerup',
          'iserror', 'percentage', 'cashamount', 'charityamount', 'count']
df = df[columns]



transactions = df.loc[(~df['balancechange'].isna()) & (df['istransaction'] == True)]
transactions.to_csv('output_tx.csv')

print(transactions.value_counts(['type']))

interested_events = ['ClaimGoalRewardSecondaryAccount','RankUp', 'Awarded']
output_csv = transactions.loc[(transactions['type'].isin(interested_events))]

jump_awards = transactions.loc[(transactions['type'] == 'Awarded')
                                &
                                (transactions['usedpowerup'] == 'TowerJump')]

non_jump_awards = transactions.loc[(transactions['type'] == 'Awarded')
                                &
                                (transactions['usedpowerup'] != 'TowerJump')]

print(jump_awards['balancechange'].sum())
print(non_jump_awards['balancechange'].sum())


output_csv.to_csv('output_2.csv')




