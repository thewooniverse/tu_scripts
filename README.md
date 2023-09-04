
# Overview





## Architecture / specifications
- What do I want the outcome of the script to be?

For each cashout;
--  A formatted text file containing different types of data
--- Verdict: Approved, Flagged(reasons), Rejected(reasons)
--- Value Source Breakdown: GameEnd%, MatchUP%, tournament wins%, GoalClaims%, MegaSpins%, AdminAddBalance%
--- Key Numbers: Take Per Game, Games to Cashout, Number of Invites, winrate;

--- Collusion: % of money from invited players;
--- Collusion / multiacc: device sharing



Final output should be a JSON file that either...
- Gets sent to the admin backend using admin API and saved onto the relevant cashout index on admin UI
- Or have a parser.py that can be called on the JSON to just pretty-print out the cashout audit logs to be manually used by liveops staff;


- What data do I want it to be fed? What data can be fed?
-- Likely, two files:
--- userid_*.csv | this gives me a csv file containing ALL events logged into the event stream

--- cashouts.csv | this file contains the history of all past cashouts, their states and notes;
----- this file is necessary to check things like duplicate payout addresses and other similar flags;



--- alternatively, we could even try to get something on a system / aggregate level;
--- for example. we grab everything from all players, how big would that be? It would however allow me to parse through things much more efficiently and effectively.



### Future Features (not in current spec):
- check invitees - API call to check the gameplay history of invited player lists


### TODO:
In order to get each of these numbers / verdicts I need to extract the different dataframes from the main dataframe.

Research / analyze what the data looks like for different players:
- Grinders / regular players --> cdyswain, Sammi, wrickmon
- Mega Inviters --> SavvyApps
- Top players --> UCFKnight77, SuperDad, Furry
- Collusive / Suspicious Players - Miykael

Check / validate scripts against manual audit notes;



1. datetime index'd dataframe -> dataframe of date range from prev cashout to current cashout timestamps
2. Gameplay dataframe
3. Tournament end dataframe
4. Goal Progress dataframe

Edit the query itself to not grab the columns that are not necessary for auditing purposes in the future.








