import os
import pandas as pd
import numpy as np




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


# Audit Function
def audit(dataframe):
    """
    audit(dataframe) is the main function called to audit a given cashout.
    The function must be passed a valid dataframe from an athena query.

    Returns: text string with the audit notes for given player's most recent cashout.
    """
    # prase and clean up the dataframe for processing
    dataframe = filter_and_order(dataframe)

    ## get the cashouts dataframe that contains all events for the given timestamps between this cashout, and last cashout.
    cashout_dataframe = get_cashout_events(dataframe)

    # get the key values from various calculation functions
    ## calculate livegame data
    money_from_livegames, n_livegames, n_livegame_wins, livegame_tpg, livegame_winrate = calc_livegame_numbers(cashout_dataframe)

    ## calculate matchup data
    money_from_matchups, n_matchups, n_matchup_wins, matchup_tpg, matchup_winrate = calc_matchup_numbers(cashout_dataframe)

    ## calculate top revenue source (opponents)
    top3_players = calc_opponent_numbers(cashout_dataframe)

    ## calculate amounts from tournament activities


    ## calculate amounts from in-game incentives - goals, mega spins, achievements, week1 prizes:
    money_from_goals, money_from_megaspins, money_from_awards, money_from_week1 = calc_goals_and_awards(cashout_dataframe)

    ## calculate amount from admin adding balance;
    

    # get other key values from given dataframe
    number_of_cashouts = int(cashout_dataframe.iloc[0]['count'])
    try_int = lambda x: int(x) if isinstance(x, (int, float)) and x > 0 else 0
    balance_carried_forward = try_int(cashout_dataframe.iloc[0]['newescrow'])
    balance_carried_in = try_int(cashout_dataframe.iloc[-1]['newescrow']) # << test this out with cashouts where I know there is some

    # construct the string and return the string




# dataframe parsing and cleanup functions:
def filter_and_order(dataframe):
    """
    cleanup(dataframe) takes a dataframe, and returns a cleaned up version of the dataframe with relevant columns in order
    as well as filtered rows based on event types.
    """
    dataframe = dataframe[COLUMNS]
    dataframe = dataframe[~dataframe['type'].isin(EXCLUDED_EVENTS)]
    return dataframe


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

    # slice the dataframe based on the timestamps
    ts1 = cashouts.index[0] # current cashout
    ts2 = pd.Timestamp('2000-01-05') # default timestamp 2 is super early (pre-dating the launch of the app itself), so it includes everything
    if len(cashouts) > 1:
        ts2 = cashouts.index[1] # last cashout, if it exists;
    current_df = dataframe.loc[ts1:ts2]
    
    return current_df




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
    livegame_winrate = calc_pct(n_livegame_wins, n_livegames)

    return money_from_livegames, n_livegames, n_livegame_wins, livegame_tpg, livegame_winrate


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
    matchup_winrate = calc_pct(n_matchup_wins, n_matchups)

    return money_from_matchups, n_matchups, n_matchup_wins, matchup_tpg, matchup_winrate


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
        (dataframe['awardedprizetype'] == 'Tokens')]

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

    return money_from_goals, money_from_megaspins, money_from_awards, money_from_week1




































# basic testing of the calculations on a test CSV file
if __name__ == '__main__':
    print("ABC")




