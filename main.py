import pandas as pd
import os
import pyperclip
import datetime
import numpy as np

"""
Assumptions:
- Current key in Athena query is username.
- In order for query to be accurate over multiple name changes on the account level, the queries need to key off of userid instead of usernames
----
Multithreading:
Performance upgrade to be able to audit multiple different accounts at once.
This current script will likely need to be refactored, and there will need to be a wrapper script that calls this script in a multithreaded manner.

V1 - is usable manually by adjusting simply the name of the target CSV file and locating it in the right directory.
V2 - will use multithreading to target and process all CSVs placed in the relevant directory, cleanup and outputting a JSON file of all the relevant audits;
V3 - will carry a auto-flagging system based on a historic dataset of all cashouts in the past.
V4 - Refactoring / performance upgrades;

add sys.argv to export parsed columns and events;
- csv filename;

Add audit process for invited players / collusion:
- The initial query CAN include inviterplayername/inviteruserid == whatever;
- Then I extract out this data into another dataframe and then it can also be used for analysis afterwards;
"""
# read the dataframes (later needs to be refactored to handle a directory full of files)
df_path = f"{os.getcwd()}{os.path.sep}CSVs{os.path.sep}DLS_inviter.csv"
df = pd.read_csv(df_path)

# select the relevant columns for cashouts in order / organization
columns = ['time', 'type', 'username', 'userid', 'deviceid', 'istransaction',
          'gameid', 'gametype', 'level', 'isbot', 'reason', 'result', 'endstatus', 'opponentusername',
          'prevbalance','newbalance', 'prevescrow', 'newescrow', 'balancechange', 'escrowchange',
          'originalprizetype', 'awardedprizetype', 'awardedprizeamount',
          'usedpowerup',
          'iserror', 'percentage', 'cashamount', 'charityamount', 'count']
df = df[columns]
str_parsed_columns = ', '.join(columns)
# print(str_parsed_columns) # this line is used when I need to get the list of columns to rewrite the Athena / SQL query

"""
UPDATED AUDIT STRING:


"""



# clean the data such as to exclude the rows that are not of interest to me;
excluded_events = ['ClientEvent', 'MakeMove','FindMatch', 'GoalProgress', 'EditUser', 'RankUp', 'SessionStart', 
                   'IssueChallenge',
                   'CashOutStart', 'CashOutMismatch', 'CancelChallenge', 'CancelMatch']
excluded_sql_str = f""
for event in excluded_events:
    excluded_sql_str+=f"AND type != \'{event}\'"

df = df[~df['type'].isin(excluded_events)]

# convert the time column to datetime and set it as index of the dataframe
df['time'] = pd.to_datetime(df['time'])
df.set_index('time', inplace=True)





# get the cashout timestamps and isolate the dataframe into events from previous cashout (or start) to current cashout;
cashouts = df.loc[(df['type'].str.contains('CashOutFinish'))
              &
             (~df['iserror'])]

## handle the duplicates if there are more CashoutFinish
if (len(cashouts)-1) != cashouts['count'][0]:
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

n_matchup_wins = livegame_df.loc[df['result'] == 'win'].shape[0]
n_matchups = matchup_df.shape[0]
matchup_tpg = round(money_from_matchups/n_matchups,2)


## calculating revenue source by player;
games_df = current_df.loc[(current_df['type'] == 'AcknowledgeResult')
                          |
                          (current_df['type'] == 'GameEnd')
                          ]

top_3_sources = games_df.groupby('opponentusername')['balancechange'].sum().sort_values(ascending=False)[0:3] # refactor this;



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

## calculate money earned from invited players through gameplay (collusion)
invited_players = list(current_df.loc[current_df['type'] == 'SetInviter', 'username'])
games_against_invited_df = current_df.loc[current_df['opponentusername'].isin(invited_players)]
invited_balance_change = games_against_invited_df['balancechange'].sum()
invited_escrow_change = games_against_invited_df['escrowchange'].sum()
invited_total = invited_balance_change + invited_escrow_change
print(invited_total)




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
                        money_from_rewards + 
                        money_from_tourneys + money_spent_on_tourneys +
                        admin_added_balance
                        )

cashout_value = cashouts['prevbalance'][0]
username = cashouts['username'][0]
note = "Invited players are hard to track down based on the way our database is currently written"


# construct the response string
response_string = f"""
--- Overview ---
Username: {username}
Cashout Value: {cashout_value} 
Audited Value: {total_won_calculated}
Audit Date: {datetime.datetime.now().strftime("%Y/%m/%d %Y:%M %p")}

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
|- Admin added: {admin_added_balance} = {calc_pct(admin_added_balance, total_won_calculated)}

--- GamePlay Analysis ---
Number of Games to Cashout (Total|Live|MatchUps): {n_livegames+n_matchups}|{n_livegames}|{n_matchups}
Livegame TPG: {livegame_tpg}
MatchUP TPG: {matchup_tpg}
Livegame winrate: {calc_pct(n_livegame_wins, n_livegames)} %
MatchUP winrate: {calc_pct(n_matchup_wins, n_matchups)}%
Money flow between invitees (amount|%): {invited_total}|{calc_pct(invited_total, total_won_calculated)}

--- NOTES ---
{note}
- All values in Pennies
- IF Audited value != Cashout value, discrepancy may be caused by:
-- Value of escrow carried in and out of cashouts
-- Direct manipulation of user data by engineers
-- If value is significant (10+%), then manual review / audits are required.
-- Discrepancies may also exist for very old players who have been playing 
"""
# print(response_string)
pyperclip.copy(response_string)











"""

--- Overview ---
Username: DLS151
Cashout Value: 270349.0 
Audited Value: 226256.0
Audit Date: 2023/09/10 2023:36 AM

Cashout Count: 0
Amount carried in (escrow): 0
Amount carreid forward (escrow): 0

Revenue Source Breakdown: 
|- Amount = % of total
|-------------
|- Mega Spins: 3150.0 = 1.0%
|- Live Games: 211906.0 = 94.0%
|- MatchUPs: 95.0 = 0.0%
|- Goals: 238.0 = 0.0%
|- Tournaments (won|spent|net): 50.0|-600.0|-550.0 = -0.0%
|- Admin added: 11417.0 = 5.0

--- GamePlay Analysis ---
Number of Games to Cashout (Total|Live|MatchUps): 25166|24330|836
Livegame TPG: 8.71
MatchUP TPG: 0.11
Livegame winrate: 63.0 %
MatchUP winrate: 0.0%
Money flow between invitees (amount|%): 16.0|0.0

--- NOTES ---
Invited players are hard to track down based on the way our database is currently written
- All values in Pennies
- IF Audited value != Cashout value, discrepancy may be caused by:
-- Value of escrow carried in and out of cashouts
-- Direct manipulation of user data by engineers
-- If value is significant (10+%), then manual review / audits are required.
-- Discrepancies may also exist for very old players who have been playing 

"""