import pandas as pd
import os
import pyperclip
import datetime

"""
Assumptions:
- Query already does the majority of column / row filtering ahead of time so that it is not neede in the script itself
-- Also, such that the load on downloading files from AWS / Athena is lower.
- In order for query to be accurate over 

add sys.argv to export parsed columns and events;
- csv filename;

Add audit process for invited players / collusion:
- The initial query CAN include inviterplayername/inviteruserid == whatever;
- Then I extract out this data into another dataframe and then it can also be used for analysis afterwards;



"""
# read the dataframes (later needs to be refactored to handle a directory full of files)
df_path = f"{os.getcwd()}{os.path.sep}a{os.path.sep}amed.csv"
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


# clean the data such as to exclude the rows that are not of interest to me;
excluded_events = ['ClientEvent', 'MakeMove','FindMatch', 'GoalProgress', 'EditUser', 'RankUp', 'SessionStart', 
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

Revenue Source Breakdown: Amount = % of whole
|- Mega Spins: {money_from_megaspin} = {calc_pct(money_from_megaspin, total_won_calculated)}%
|- Live Games: {money_from_livegames} = {calc_pct(money_from_livegames, total_won_calculated)}%
|- MatchUPs: {money_from_matchups} = {calc_pct(money_from_matchups, total_won_calculated)}%
|- Goals: {money_from_rewards} = {calc_pct(money_from_rewards, total_won_calculated)}%
|- Tournaments (won|spent|net): {money_from_tourneys} | {money_spent_on_tourneys} {money_from_tourneys+money_spent_on_tourneys} = {calc_pct((money_from_tourneys+money_spent_on_tourneys), total_won_calculated)}%
|- Admin added: {admin_added_balance} = {calc_pct(admin_added_balance, total_won_calculated)}

--- GamePlay Analysis ---
Number of Games to Cashout (Total|Live|MatchUps): {n_livegames+n_matchups}|{n_livegames}|{n_matchups}
Livegame TPG: {livegame_tpg}
MatchUP TPG: {matchup_tpg}
Livegame winrate: {calc_pct(n_livegame_wins, n_livegames)} %
MatchUP winrate: {calc_pct(n_matchup_wins, n_matchups)}%


--- NOTES ---
- {note}
- All values in pennies
- If value between Audited value != Cashout value, there may be some value unaccounted for;
- If the amount is not great, we can ignore it, however, if it is significant, this requires manual review.
-- The discrepancies are most frequently from balance carreid into the cashout usually from tournament entry fees, admin adding balances and 
-- In time, these will be accounted for;
"""
# print(response_string)
pyperclip.copy(response_string)

print(current_df.iloc[-1])











"""

--- Overview ---
Username: AMgudito23
Cashout Value: 1120.0 
Audited Value: 1122.0
Audit Date: 2023/09/09 2023:55 AM

Revenue Source Breakdown: Amount = % of whole
|- Mega Spins: 50.0 = 4.0%
|- Live Games: 1072.0 = 96.0%
|- MatchUPs: -4.0 = -0.0%
|- Goals: 4.0 = 0.0%
|- Tournaments (won|spent|net): 0.0 | 0.0 0.0 = 0.0%
|- Admin added: 0.0 = 0.0

--- GamePlay Analysis ---
Number of Games to Cashout (Total|Live|MatchUps): 250|248|2
Livegame TPG: 4.32
MatchUP TPG: -2.0
Livegame winrate: 56.0 %
MatchUP winrate: 0.0%


--- NOTES ---
- Invited players are hard to track down based on the way our database is currently written
- All values in pennies
- If value between Audited value != Cashout value, there may be some value unaccounted for;
- If the amount is not great, we can ignore it, however, if it is significant, this requires manual review.
-- The discrepancies are most frequently from balance carreid into the cashout usually from tournament entry fees, admin adding balances and 
-- In time, these will be accounted for;

"""