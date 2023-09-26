import pandas as pd
import numpy as np
import os


csvs_path = os.path.join(os.path.dirname(__file__), "edge_case.csv")
df = pd.read_csv(csvs_path, low_memory=False)
print(df)



# convert the time column to datetime and set it as index of the dataframe
df['time'] = pd.to_datetime(df['time'])
df.set_index('time', inplace=True)


# get the cashout timestamps and isolate the dataframe into events from previous cashout (or start) to current cashout;
cashouts = df.loc[(df['type'].str.contains('CashOutFinish'))
              &
             (~df['iserror'])]

## handle the duplicates if there are more CashoutFinish
if (len(cashouts)-1) != cashouts['count'].iloc[0]:
    cashouts.drop_duplicates(subset=['count'], inplace=True)
    # this needs to be tested as there were some errors in the past caused by faulty cashoutfinish events

ts1 = cashouts.index[0] # current cashout
ts2 = pd.Timestamp('2000-01-05') # default timestamp 2 is super early (pre-dating the launch of the app itself), so it includes everything
if len(cashouts) > 1:
    ts2 = cashouts.index[1] # last cashout, if it exists;

## slice the df based on the two timestamps
current_df = df.loc[ts1:ts2]

print(current_df)


livegame_df = current_df.loc[
    current_df['type'] == 'GameEnd']
livegame_results = livegame_df['balancechange'] + (livegame_df['escrowchange'])
money_from_livegames = livegame_results.sum()
print(money_from_livegames)










## calculating the financial value claimed in rewards;
rewards_df = current_df.loc[
    (current_df['type'] == 'ClaimGoalReward')
               &
               ((current_df['istransaction'] == True)
                |
                (current_df['awardedprizetype'] == 'Tokens')
                )]

money_from_rewards = rewards_df['balancechange'].sum()


## calculating money from mega spins
megaspin_df = current_df.loc[
    (current_df['type'] == 'Awarded')
    &
    (current_df['usedpowerup'] == 'TowerJump')]

money_from_megaspin = megaspin_df['balancechange'].sum()


non_jump_awards = current_df.loc[
    (current_df['type'] == 'Awarded')
    &
    (current_df['usedpowerup'] != 'TowerJump')]

money_from_awards = non_jump_awards['balancechange'].sum()


## calculate money from week1 activities
week1_df = current_df.loc[
    (current_df['type'] == 'ClaimWeek1Prize')
    &
    (current_df['istransaction'] == True)]
week1_total = week1_df['balancechange'].sum()

print(money_from_rewards, money_from_awards, money_from_megaspin, week1_total)






