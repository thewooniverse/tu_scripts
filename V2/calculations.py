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










# basic testing of the calculations
if __name__ == '__main__':
    print("ABC")




