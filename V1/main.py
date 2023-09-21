import pandas as pd
import os
import pyperclip
import datetime
import sys
import numpy as np

"""
NOTE:

Assumptions:
- Current key in Athena query is username.
- In order for query to be accurate over multiple name changes on the account level, the queries need to key off of userid instead of usernames
- This can be done manually through Athena
----

TODO:

"""


# read the targeted dataframes (later needs to be refactored to handle a directory full of files)

## ensure that paths and files exist:
csvs_path = f"{os.getcwd()}{os.path.sep}CSVs"
if not os.path.exists(csvs_path):
    os.mkdir(csvs_path)

name = "wrickmon_9192023" # this is the name of the file and what you want to change in V1
df_path = f"{csvs_path}{os.path.sep}{name}.csv"
df = pd.read_csv(df_path, low_memory=False)


# select the relevant columns for cashouts in order / organization
columns = ['time', 'type', 'username', 'userid', 'deviceid', 'istransaction',
          'gameid', 'gametype', 'level', 'isbot', 'reason', 'result', 'endstatus', 'opponentusername',
          'prevbalance','newbalance', 'prevescrow', 'newescrow', 'balancechange', 'escrowchange',
          'originalprizetype', 'awardedprizetype', 'awardedprizeamount',
          'usedpowerup',
          'iserror', 'percentage', 'cashamount', 'charityamount', 'count']
df = df[columns]



# clean the data such as to exclude the rows that are not of interest to me;
excluded_events = ['ClientEvent', 'MakeMove', 'GameBegin', 'AdStart', 
                   'FindMatch', 'GoalProgress', 'EditUser', 'RankUp', 'SessionStart', 
                   'CashOutStart', 'CashOutMismatch', 'AdFinish',
                   'CancelChallenge', 'CancelMatch', 'AcceptChallenge', 'IssueChallenge', 'DeclineChallenge']





# constructing the Athena query to current_query.txt
str_parsed_columns = ', '.join(columns)
excluded_sql_str = f""
for event in excluded_events:
    excluded_sql_str+=f"\nAND type != \'{event}\'"

query_str = f"""
/* GENERAL - CASHOUT AUDIT TABLE */
/* Manual input variables marked between [brackets] - please input [USERNAME] [DEST_SERVER], remove the brackets after input */

SELECT {str_parsed_columns}
FROM [DEST_SERVER]
WHERE (username = '[USERNAME]' OR inviterusername = '[USERNAME]')
{excluded_sql_str}
ORDER BY time DESC
"""

with open(f"{os.getcwd()}{os.path.sep}current_query.txt", 'w') as wf:
    wf.write(query_str)






# ex lude the dataframe to exclude excluded events;
df = df[~df['type'].isin(excluded_events)]

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





# calculate the different money sources

## calculating amount of money won from live games, livegame TPG, winrates and levels
livegame_df = current_df.loc[
    current_df['type'] == 'GameEnd']
livegame_results = livegame_df['balancechange'] + (livegame_df['escrowchange'])
money_from_livegames = livegame_results.sum()

# calculate take per game
n_livegame_wins = livegame_df.loc[df['reason'] == 'win'].shape[0]
n_livegames = livegame_df.shape[0]
livegame_tpg = round(money_from_livegames/n_livegames,2)


## calculating take per game from matchup games;
matchup_df = current_df.loc[
    current_df['type'] == 'AcknowledgeResult']

matchup_results = matchup_df['balancechange'] + matchup_df['escrowchange']
money_from_matchups = matchup_results.sum()

n_matchup_wins = matchup_df.loc[df['result'] == 'win'].shape[0]
n_matchups = matchup_df.shape[0]


if (money_from_matchups == 0) or (n_matchups == 0):
    matchup_tpg = 0
else:
    matchup_tpg = round(money_from_matchups/n_matchups,2)



## calculating revenue source by player;
games_df = current_df.loc[(current_df['type'] == 'AcknowledgeResult')
                          |
                          (current_df['type'] == 'GameEnd')
                          ]
games_df = games_df.copy()
games_df.loc[:, 'results'] = games_df['balancechange'] + games_df['escrowchange']
top_3_sources = games_df.groupby('opponentusername')['results'].sum().sort_values(ascending=False)[0:3] # refactor this;
top3_dict = top_3_sources.to_dict()
top3_str = ""
for key,value in top3_dict.items():
    top3_str += f"{key}: {value}\n"



## calculating the financial value claimed in rewards;
rewards_df = current_df.loc[
    (current_df['type'] == 'ClaimGoalReward')
               &
               (current_df['awardedprizetype'] == 'Tokens')]

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









## calculate money earned from invited players through gameplay (collusion)
invited_players = list(current_df.loc[current_df['type'] == 'SetInviter', 'username'])
games_against_invited_df = current_df.loc[current_df['opponentusername'].isin(invited_players)]
invited_balance_change = games_against_invited_df['balancechange'].sum()
invited_escrow_change = games_against_invited_df['escrowchange'].sum()
invited_total = invited_balance_change + invited_escrow_change




## calculate money from tournament activities
tournament_wins_df = current_df.loc[
    (current_df['type'] == 'ClaimSpecialEventPrize')
    &
    (current_df['istransaction'] == True) # this line excludes banana / secondary currency payouts
    ]
money_from_tourneys = tournament_wins_df['balancechange'].sum()


## calculate the monies spent on tournament activities;
tournaments_spend_df = current_df.loc[
    (current_df['type'] == 'JoinSpecialEvent')
    &
    (current_df['istransaction'] == True)
]
money_spent_on_tourneys = tournaments_spend_df['balancechange'].sum()


## calculate money from admin adding balance
admin_add_bal_df = current_df.loc[
    (current_df['type'] == 'AdminAddBalance')
    &
    (current_df['istransaction'] == True)
]
admin_added_balance = admin_add_bal_df['balancechange'].sum()







## other key numbers;
number_of_cashouts = int(current_df.iloc[0]['count'])
try_int = lambda x: int(x) if isinstance(x, (int, float)) and x > 0 else 0
balance_carried_forward = try_int(current_df.iloc[0]['newescrow'])
balance_carried_in = try_int(current_df.iloc[-1]['newescrow'])
# balance_carried_forward = int(current_df.iloc[0]['newescrow'])











def calc_pct(indiv, group):
    try:
        return round((indiv / group) * 100,0)
    except ZeroDivisionError:
        return 0

# check the balances
total_won_calculated = (money_from_megaspin + 
                        money_from_matchups + 
                        money_from_livegames + 
                        money_from_rewards + week1_total +
                        money_from_tourneys + money_spent_on_tourneys +
                        admin_added_balance + money_from_awards
                        )

cashout_value = cashouts['prevbalance'].iloc[0]
username = cashouts['username'].iloc[0]

# construct the response string
response_string = f"""
--- Overview ---
Username: {username}
Cashout Value: {cashout_value} 
Audited Value: {total_won_calculated}
Audit Date: {datetime.datetime.now().strftime("%Y/%m/%d %Y:%M %p")}
Cashout Date: {ts1}
Prev Cashout Date: {ts2}

Cashout Count: {number_of_cashouts}
Amount carried in (escrow): {balance_carried_forward}
Amount carreid forward (escrow): {balance_carried_in}

Revenue Source Breakdown:
|- Amount = % of total
|-------------
|- Mega Spins: {money_from_megaspin} = {calc_pct(money_from_megaspin, total_won_calculated)}%
|- Live Games: {money_from_livegames} = {calc_pct(money_from_livegames, total_won_calculated)}%
|- MatchUPs: {money_from_matchups} = {calc_pct(money_from_matchups, total_won_calculated)}%
|- Goals: {money_from_rewards} = {calc_pct(money_from_rewards, total_won_calculated)}%
|- Tournaments (won|spent|net): {money_from_tourneys}|{money_spent_on_tourneys}|{money_from_tourneys+money_spent_on_tourneys} = {calc_pct((money_from_tourneys+money_spent_on_tourneys), total_won_calculated)}%
|- Admin added: {admin_added_balance} = {calc_pct(admin_added_balance, total_won_calculated)}%
|- Week1 prize(old): {week1_total} = {calc_pct(week1_total, total_won_calculated)}%
|- Awarded (misc): {money_from_awards} = {calc_pct(money_from_awards, total_won_calculated)}%


--- GamePlay Analysis ---
Number of Games to Cashout (Total|Live|MatchUps): {n_livegames+n_matchups}|{n_livegames}|{n_matchups}
Livegame TPG: {livegame_tpg}
MatchUP TPG: {matchup_tpg}
Livegame winrate: {calc_pct(n_livegame_wins, n_livegames)} %
MatchUP winrate: {calc_pct(n_matchup_wins, n_matchups)}%
Money flow between invitees (amount|%): {invited_total}|{calc_pct(invited_total, total_won_calculated)}%
Top 3 players won against:
{top3_str}


--- NOTES ---
- All values in Pennies
- IF Audited value != Cashout value, discrepancy may be caused by:
-- Value of escrow carried in and out of cashouts
-- Direct manipulation of user data by engineers (to add or subtract balances)
-- Some old players may have gameplay history that is logged in a different way, these are not considered.
-- Some players may have had their username changed, in this case, re-do the query but with their [userid] as the key to the query than [username]

-- If value is significant (10+%), then manual review / audits are required.
"""
# print(response_string)
pyperclip.copy(response_string)



