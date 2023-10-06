import os
import pandas as pd
import numpy as np
import datetime
import hashlib



"""
TODO:
"""



######################## Define Constants ######################## 

COLUMNS = ['time', 'type', 'username', 'userid', 'deviceid', 'istransaction',
          'gameid', 'gametype', 'level', 'isbot', 'reason', 'result', 'endstatus', 'opponentusername',
          'prevbalance','newbalance', 'prevescrow', 'newescrow', 'balancechange', 'escrowchange',
          'originalprizetype', 'awardedprizetype', 'awardedprizeamount',
          'usedpowerup',
          'iserror', 'percentage', 'cashamount', 'charityamount', 'count']

EXCLUDED_EVENTS = ['ClientEvent', 'MakeMove', 'GameBegin', 'AdStart', 
                   'FindMatch', 'GoalProgress', 'EditUser', 'RankUp', 'SessionStart', 
                   'CashOutStart', 'CashOutMismatch', 'AdFinish',
                   'CancelChallenge', 'CancelMatch', 'AcceptChallenge', 'IssueChallenge', 'DeclineChallenge']


FLAG_THRESHOLD = { #THRESHOLD holds the "info_type": "threshold" pairs. Info_type being the datapoint checked,
                   # and "threshold" being the value over which it is flagged.
                   "pct_matchup": 50, # if they won above 50% of cashout from matchups
                   "pct_admin": 50, # if they won above 50% of cashout from admin
                   'matchup_wr': 70, # if their matchup winrate is above 70% (ignore if less than 10 games)
                   'livegame_wr': 75, # if their livegames winrate is above 75%
                   'invite_pct': 25, # if they won above 60% of cashout from gameplay with invited players
                   'livegame_tpg': 10, # if their take per game is above 10cents.
                   'matchup_tpg': 35, # if their take per game is above 10cents.
                   "current_year_cash_taken": 60000, # if the user has taken more than 600$ worth of cash from the system, flag for W9

                   # eventually, this data should be imported and calculated from a database file (csv or otherwise)
                   # that is continuously added to (and therefore, the number dynmaically reflects) with each audit that is run.
}



EMAIL_HASH = "****" # please replace with hash from slack.





# Audit Function
def audit(dataframe):
    """
    audit(dataframe) is the main function called to audit a given cashout.
    The function must be passed a valid dataframe from an athena query.

    Returns: text string with the audit notes for given player's most recent cashout.
    """
    # prase and clean up the dataframe for processing, reset the indexes
    dataframe = filter_and_order(dataframe)
    dataframe['time'] = pd.to_datetime(dataframe['time'])
    dataframe.set_index('time', inplace=True)

    # get lifetime data
    ## get the lifetime cashout data
    total_cashed_out_value, total_taken_in_cash, total_donated, total_taken_in_cash_current_year = get_lifetime_cashout_data(dataframe)

    ## get lifetime gameplay data
    lifetime_money_from_livegames, lifetime_n_livegames, lifetime_n_livegame_wins, lifetime_livegame_tpg = calc_livegame_numbers(dataframe)


    # get cashout specific data
    ## get the cashouts dataframe that contains all events for the given timestamps between this cashout, and last cashout.
    cashout_dataframe, ts1, ts2, cashout_value, username = get_cashout_events(dataframe)

    # get the key values from various calculation functions
    ## calculate livegame data
    money_from_livegames, n_livegames, n_livegame_wins, livegame_tpg = calc_livegame_numbers(cashout_dataframe)

    ## calculate matchup data
    money_from_matchups, n_matchups, n_matchup_wins, matchup_tpg = calc_matchup_numbers(cashout_dataframe)

    ## calculate top revenue source (opponents & invited players)
    top3_players = calc_opponent_numbers(cashout_dataframe)
    invited_players, invited_total = calc_invited_gameplay_numbers(cashout_dataframe)

    ## calculate amounts from tournament activities
    money_from_tourneys, money_spent_on_tourneys, total_flow_tourneys = calc_tournament_outcomes(cashout_dataframe)

    ## calculate amounts from in-game incentives - goals, mega spins, achievements, week1 prizes:
    money_from_goals, money_from_megaspins, money_from_awards, money_from_week1, money_from_admin = calc_goals_and_awards(cashout_dataframe)


    # get other key values from given dataframe
    number_of_cashouts = int(cashout_dataframe.iloc[0]['count'])
    try_int = lambda x: int(x) if isinstance(x, (int, float)) and x > 0 else 0
    balance_carried_forward = try_int(cashout_dataframe.iloc[0]['newescrow'])
    balance_carried_in = try_int(cashout_dataframe.iloc[-1]['newescrow']) # << test this out with cashouts where I know there is some
    total_audited_value = money_from_livegames + money_from_matchups + total_flow_tourneys + money_from_goals + money_from_megaspins + money_from_awards + money_from_week1 + money_from_admin



    # run the flags and checks:
    check_pairs = { #key-value pair of "data": ["value", (flagged-True|False), default is false)"], that will be used by check_flag()
        "pct_matchup": [calc_pct(money_from_matchups, total_audited_value), False],
        "pct_admin": [calc_pct(money_from_admin, total_audited_value), False],
        'invite_pct': [calc_pct(invited_total, total_audited_value), False],
        "matchup_wr": [calc_pct(n_matchup_wins, n_matchups), False],
        "livegame_wr": [calc_pct(n_livegame_wins, n_livegames), False],
        "livegame_tpg": [livegame_tpg, False],
        "matchup_tpg": [matchup_tpg, False],
        "current_year_cash_taken": [total_taken_in_cash_current_year, False]
    }
    # loop through check_pairs.items(), if it returns True ->  and edit the value for the key.
    flagged_flags = []

    for key,value in check_pairs.items():
        value, flag = check_flag(key,value)
        if flag:
            flagged_flags.append(key)
            check_pairs[key] = f"|>|>{value[0]}|>|>"
        else:
            check_pairs[key] = f"{value[0]}"
    
    status = determine_status(flagged_flags)

    flag_strings = ""
    for flag in flagged_flags:
        flag_string = f"\n{flag}: {get_flag_message(flag)}"
        flag_strings += flag_string



    # construct the string and return the string
    response_string = f"""\n\n
---OVERVIEW---
Username: {username}
Cashout Value: {cashout_value} 
Audited Value: {total_audited_value}
Audit Date: {datetime.datetime.now().strftime("%Y/%m/%d %Y:%M %p")}
Cashout Date: {ts1}
Prev Cashout Date: {ts2}
----------
Lifetime Cashouts total | cash | donated: {total_cashed_out_value} | {total_taken_in_cash} | {total_donated}
Cash taken this year: {check_pairs['current_year_cash_taken']}
Lifetime Livegame TPG: {lifetime_livegame_tpg}
Lifetime Livegames played: {lifetime_n_livegames}
Lifetime Livegame Winrate: {calc_pct(lifetime_n_livegame_wins,lifetime_n_livegames)} %
Cashout Count: {number_of_cashouts}
----------
Audit Bot Verdict: {status}
{flag_strings}
----------

Cashout Source Breakdown:
|- Amount = % of total won
|-------------
|- Mega Spins: {money_from_megaspins} = {calc_pct(money_from_megaspins, total_audited_value)}%
|- Live Games: {money_from_livegames} = {calc_pct(money_from_livegames, total_audited_value)}%
|- MatchUPs: {money_from_matchups} = {check_pairs['pct_matchup']}%
|- Goals: {money_from_goals} = {calc_pct(money_from_goals, total_audited_value)}%
|- Tournaments (won|spent|net): {money_from_tourneys}|{money_spent_on_tourneys}|{total_flow_tourneys} = {calc_pct(total_flow_tourneys, total_audited_value)}%
|- Admin added: {money_from_admin} = {check_pairs['pct_admin']}%
|- Week1 prize(old): {money_from_week1} = {calc_pct(money_from_week1, total_audited_value)}%
|- Awarded (misc): {money_from_awards} = {calc_pct(money_from_awards, total_audited_value)}%
|
|- Amount carried into cashout (escrow): {balance_carried_forward}
|- Amount carreid forward to next (escrow): {balance_carried_in}


--- GamePlay Analysis ---
Number of Games to Cashout (Total|Live|MatchUps): {n_livegames+n_matchups}|{n_livegames}|{n_matchups}
Livegame TPG: {check_pairs['livegame_tpg']}
MatchUP TPG: {check_pairs['matchup_tpg']}
Livegame winrate: {check_pairs['livegame_wr']} %
MatchUP winrate: {check_pairs['matchup_wr']}%
Top 3 players won against:
{top3_players}

Invited Players: {invited_players}
Money flow between invitees (amount|%): {invited_total}|{check_pairs['invite_pct']}%\n\n
"""
    return response_string





# helper functions
def anonymize_email(email, secret_key=EMAIL_HASH):
    """
    anonymize_email(email, secret_key): Anonymize an email using a hash function and a secret key.

    Args:
    - email (str): Email address to be anonymized.
    - secret_key (str): Secret key used for anonymization.

    Returns:
    - str: Anonymized hash representation of the email.
    """
    
    # Combine the email and the secret key
    combined = str(email) + str(secret_key)
    
    # Generate a SHA256 hash of the combined string
    hash_obj = hashlib.sha256(combined.encode())
    
    # Return the hexadecimal representation of the hash
    return hash_obj.hexdigest()

def reconcile_email_hash():
    """
    reconcile_email_hash():
    """
    pass






# flagging functions
def get_flag_message(flag):
    flag_messages = { #THRESHOLD holds the "info_type": "threshold" pairs. Info_type being the datapoint checked,
                   # and "threshold" being the value over which it is flagged.
                   "pct_matchup": "Too much money has been won in matchups", # if they won above 50% of cashout from matchups
                   "pct_admin": "Too much money has been added by admins", # if they won above 50% of cashout from admin
                   'matchup_wr': "Winrate is too high (ignore if # of games is not too high)", # if their matchup winrate is above 70% (ignore if less than 10 games)
                   'livegame_wr': "Winrate is too high", # if their livegames winrate is above 75%
                   'invite_pct': "Too much money won against invited players", # if they won above 60% of cashout from gameplay with invited players
                   'livegame_tpg': "Take per game is too high, user is too profitable", # if their take per game is above 10cents.
                   'matchup_tpg': "Take per game is too high, user is too profitable", # if their take per game is above 10cents.
                   "current_year_cash_taken": "User needs to sign a W9 form or prove they are outside of US",
                   # eventually, this data should be imported and calculated from a database file (csv or otherwise)
                   # that is continuously added to with each audit that is run.
                    }
    return flag_messages[flag]

def check_flag(info_type, value):
    """
    check_flag(info_type, value) - checks the flag and sees whether the value for that flag is passed 
    value passed are as follows: [value, flagged?-True|False]
    """
    return [value, value[0] > FLAG_THRESHOLD[info_type]]


def determine_status(flags):
    if len(flags) == 0:
        return f"Approved"
    elif len(flags) <= 3:
        return f"Flagged for: {', '.join(flags)}"
    elif len(flags) > 3:
        return f"Rejected for: {', '.join(flags)}"


# dataframe parsing and cleanup functions:
def filter_and_order(dataframe):
    """
    cleanup(dataframe) takes a dataframe, and returns a cleaned up version of the dataframe with relevant columns in order
    as well as filtered rows based on event types.
    """
    dataframe = dataframe[COLUMNS]
    dataframe = dataframe[~dataframe['type'].isin(EXCLUDED_EVENTS)]
    return dataframe


def get_year_timestamps():
    """
    get_year_timestamps(): Get the current year, timestamp for the beginning of the year, 
    and timestamp for the end of the year.
    
    Returns:
    - int: Current year
    - datetime: Timestamp of the beginning of the year
    - datetime: Timestamp of the end of the year
    """
    
    # Get the current date and time
    now = datetime.datetime.now()
    
    # Get the beginning of the current year (January 1st, 00:00:00)
    start_of_year = datetime.datetime(year=now.year, month=1, day=1)
    
    # Get the end of the current year (December 31st, 23:59:59)
    end_of_year = datetime.datetime(year=now.year, month=12, day=31, hour=23, minute=59, second=59)
    
    return now.year, start_of_year, end_of_year



def get_cashout_events(dataframe):
    """
    get_cashout_events(dataframe) takes a dataframe, and returns a sliced version of the dataframe, containing all events
    leading up to the current cashout, from the timestamp of the last cashout.
    """
    # get a table of non-error cashoutfinish events
    cashouts = dataframe.loc[(dataframe['type'].str.contains('CashOutFinish'))
              &
              (~dataframe['iserror'])]
    
    # handle the duplicates if there are more CashoutFinish
    if (len(cashouts)-1) != cashouts['count'].iloc[0]:
        cashouts.drop_duplicates(subset=['count'], inplace=True)

    # slice the dataframe based on the timestamps to get the cashout events for the given cashout to last cashout
    ts1 = cashouts.index[0] # current cashout
    if len(cashouts) > 1:
        ts2 = cashouts.index[1] # last cashout, if it exists;
    else:
        ts2 = pd.Timestamp('2000-01-05') # default timestamp 2 is super early (pre-dating the launch of the app itself), so it includes everything
    current_df = dataframe.loc[ts1:ts2]

    # get the other necessary numbers / metadata
    cashout_value = cashouts['prevbalance'].iloc[0]
    username = cashouts['username'].iloc[0]
    
    return current_df, ts1, ts2, cashout_value, username


def get_lifetime_cashout_data(dataframe):
    """
    get_lifetime_cashout_data(dataframe): takes a dataframe, and returns lifetime cashout data around number of cashouts, total cashed out and W9 / Tax implications and data.
    """
    # get the cashouts table and extract data
    cashouts = dataframe.loc[(dataframe['type'].str.contains('CashOutFinish'))
              &
              (~dataframe['iserror'])]
    
    # drop duplicates
    if (len(cashouts)-1) != cashouts['count'].iloc[0]:
        cashouts.drop_duplicates(subset=['count'], inplace=True)
    
    # get lifetime cashout data
    total_cashed_out_value = cashouts['prevbalance'].sum()
    total_taken_in_cash = cashouts['cashamount'].sum()
    total_donated = cashouts['charityamount'].sum()

    # calculate how much they took in cash this year
    
    current_year, year_start_ts, year_end_ts = get_year_timestamps()
    current_cashout_ts = cashouts.index[0]
    
    current_year_cashouts = cashouts.loc[year_start_ts:year_end_ts]
    total_taken_in_cash_current_year = current_year_cashouts['cashamount'].sum()

    return total_cashed_out_value, total_taken_in_cash, total_donated, total_taken_in_cash_current_year





def get_session_data():
    """
    session_data(): takes a gameplay dataframe and calculates their session data to determine whether they appear to be a bot or not.

    returns:
    - avg_playtime_per_day
    - avg_games_per_day
    - avg_length_of_gameplay
    """
    pass
    

    



# calculation functions for various important numbers / aspects of gameplay and sources of money
def calc_pct(indiv, group):
    """
    calc_pct(indiv, group) - takes two int/float types as individual and group (parent) number and returns 
    the percent that the individual is of the group number.
    """
    try:
        return round((indiv / group) * 100,0)
    except ZeroDivisionError:
        return 0

def calc_tpg(amt, n_games):
    """
    calc_tpg(amt, n_games) - takes two int/float types as amt and n_games, and returns the take per game based on these numbers
    with error handling on when zero values are passed.
    """
    try:
        return round(amt/n_games, 2)
    except ZeroDivisionError:
        return 0


def calc_livegame_numbers(dataframe):
    """
    calc_livegame_numbers(dataframe) - takes a dataframe containing events for a cashout
    and returns list of key values regarding livegames of the given cashout.
    
    returns: total_money_flow, n_games, n_wins, tpg, winrate
    """
    livegames_df = dataframe.loc[dataframe['type'] == 'GameEnd']
    livegame_results = livegames_df['balancechange'] + (livegames_df['escrowchange'])
    money_from_livegames = livegame_results.sum()

    # calculate take per game
    n_livegame_wins = livegames_df.loc[livegames_df['reason'] == 'win'].shape[0]
    n_livegames = livegames_df.shape[0]

    livegame_tpg = calc_tpg(money_from_livegames, n_livegames)

    return money_from_livegames, n_livegames, n_livegame_wins, livegame_tpg


def calc_matchup_numbers(dataframe):
    """
    calc_matchup_numbers(dataframe) - takes a dataframe containing events for a cashout
    and returns list of key values regarding matchup games of the given cashout.
    
    returns: total_money_flow, n_games, n_wins, tpg, winrate
    """
    matchup_df = dataframe.loc[dataframe['type'] == 'AcknowledgeResult']

    matchup_results = matchup_df['balancechange'] + matchup_df['escrowchange']
    money_from_matchups = matchup_results.sum()
    n_matchup_wins = matchup_df.loc[matchup_df['result'] == 'win'].shape[0]
    n_matchups = matchup_df.shape[0]

    matchup_tpg = calc_tpg(money_from_matchups, n_matchups)

    return money_from_matchups, n_matchups, n_matchup_wins, matchup_tpg


def calc_opponent_numbers(dataframe):
    """
    calc_opponent_numbers(dataframe) - takes a dataframe containing events for a cashout
    and returns a list of key values regarding who the given player has won the most amount of money from

    returns: top x players in string format
    """
    games_df = dataframe.loc[(dataframe['type'] == 'AcknowledgeResult')
                          |
                          (dataframe['type'] == 'GameEnd')]
    games_df = games_df.copy()
    games_df.loc[:, 'results'] = games_df['balancechange'] + games_df['escrowchange']
    top_3_sources = games_df.groupby('opponentusername')['results'].sum().sort_values(ascending=False)[0:3]
    top3_dict = top_3_sources.to_dict()
    top3_str = ""
    for key,value in top3_dict.items():
        top3_str += f"{key}: {value}\n"

    return top3_str

def calc_invited_gameplay_numbers(dataframe):
    """
    calc_opponent_numbers(dataframe) - takes a dataframe containing events for a cashout
    and returns a list of key values regarding who the given player has won the most amount of money from

    returns: top x players in string format
    """
    invited_players = list(dataframe.loc[dataframe['type'] == 'SetInviter', 'username'])
    games_against_invited_df = dataframe.loc[dataframe['opponentusername'].isin(invited_players)]
    invited_balance_change = games_against_invited_df['balancechange'].sum()
    invited_escrow_change = games_against_invited_df['escrowchange'].sum()
    invited_total = invited_balance_change + invited_escrow_change

    return invited_players, invited_total




def calc_goals_and_awards(dataframe):
    """
    calc_goals_and_awards(dataframe) - takes a dataframe containing events for a cashout
    returns a list of key values regarding goals and awards that contributed to cashout.

    returns: money_from_goals, money_from_megaspins, money_from_awards, money_from_week1
    """

    # calculate goals
    goals_df = dataframe.loc[
        (dataframe['type'] == 'ClaimGoalReward')
        &
        (
            (dataframe['istransaction'] == True)
            |
            (dataframe['awardedprizetype'] == 'Tokens'))]
    
    money_from_goals = goals_df['balancechange'].sum()

    # calculate mega spins
    megaspin_df = dataframe.loc[
        (dataframe['type'] == 'Awarded')
        &
        (dataframe['usedpowerup'] == 'TowerJump')]

    money_from_megaspins = megaspin_df['balancechange'].sum()

    # calculate awards
    non_jump_awards = dataframe.loc[
        (dataframe['type'] == 'Awarded')
        &
        (dataframe['usedpowerup'] != 'TowerJump')]

    money_from_awards = non_jump_awards['balancechange'].sum()

    # calculate money from week 1
    week1_df = dataframe.loc[
        (dataframe['type'] == 'ClaimWeek1Prize')
        &
        (dataframe['istransaction'] == True)]
    
    money_from_week1 = week1_df['balancechange'].sum()

    # calculate admin adding balance
    admin_add_bal_df = dataframe.loc[
        (dataframe['type'] == 'AdminAddBalance')
        &
        (dataframe['istransaction'] == True)]
    admin_added_balance = admin_add_bal_df['balancechange'].sum()


    return money_from_goals, money_from_megaspins, money_from_awards, money_from_week1, admin_added_balance


def calc_tournament_outcomes(dataframe):
    """
    calc_tournament_outcomes(dataframe) - takes a dataframe containing events for a cashout
    and returns list of key values regarding tournament activity
    
    returns: money_from_tourneys, money_spent_on_tourneys, total_flow_tourneys    
    """
    ## calculate money from tournament activities
    tournament_wins_df = dataframe.loc[
        (dataframe['type'] == 'ClaimSpecialEventPrize')
        &
        (dataframe['istransaction'] == True) # this line excludes banana / secondary currency payouts
        ]
    money_from_tourneys = tournament_wins_df['balancechange'].sum()


    ## calculate the monies spent on tournament activities;
    tournaments_spend_df = dataframe.loc[
        (dataframe['type'] == 'JoinSpecialEvent')
        &
        (dataframe['istransaction'] == True)
    ]
    money_spent_on_tourneys = tournaments_spend_df['balancechange'].sum()

    ## calculate the net flow
    total_flow_tourneys = money_from_tourneys + money_spent_on_tourneys

    return money_from_tourneys, money_spent_on_tourneys, total_flow_tourneys
    




# Athena Query construction function:
def refresh_athena_query():
    """
    refresh_athena_query(columns, excl_events): refreshes current_query.txt with the newest Athena query based on the current state of columns and excluded events
    """
    str_parsed_columns = ', '.join(COLUMNS)
    excluded_sql_str = f""
    for event in EXCLUDED_EVENTS:
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
    script_path = f'{os.path.sep}'.join(__file__.split(f'{os.path.sep}')[:-1])
    with open(script_path + os.path.sep + "current_query.txt", 'w') as wf:
        wf.write(query_str)















# basic testing of the calculations on a test CSV file
if __name__ == '__main__':
    script_path = f'{os.path.sep}'.join(__file__.split(f'{os.path.sep}')[:-1])


    # # test the audit(dataframe) function
    # test_path = script_path + os.path.sep + "CSVs" + os.path.sep + "DLS_inviter.csv"
    # test_data = pd.read_csv(test_path)
    # print(audit(test_data))

    # # refresh the audit queries in case it has changed.
    # refresh_athena_query()






    # section for constructing initial email_hash.csv table containing history of hashed "username, email_hash"" columns
    """
    In order to update this, execute the following query, download the results and put it in the current directory where script is (CASHOTU_AUDITING/V2/) as email_pre_hash.csv:
    The hash itself will be shared on slack, please replace it.

/* GENERAL - username-payee table */
/* Manual input variables marked between [brackets] - please input [USERNAME] [DEST_SERVER], remove the brackets after input */

SELECT username, payee
WHERE type = 'CashoutFinish'
AND NOT iserror
ORDER BY time DESC

    """
    ## read the CSV into pd
    unhashed_emails_csv_path = script_path + os.path.sep + "email_pre_hash.csv"
    unhashed_email_df = pd.read_csv(unhashed_emails_csv_path)
    # print(unhashed_email_df)

    ## apply the hashing function to the payee column, and drop the column
    unhashed_email_df['email_hash'] = unhashed_email_df['payee'].apply(anonymize_email)
    hashed_email_df = unhashed_email_df[['username', 'email_hash']]
    print(hashed_email_df)

    ## save the new dataframe into email_hash.xlsx
    email_hash_csv_dest_path = script_path + os.path.sep + "email_hash.csv"
    hashed_email_df.to_csv(email_hash_csv_dest_path)








