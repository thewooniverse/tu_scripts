
# Overview
Cashout auditing scripts intended to automate and streamline this liveops process.

V1 - is usable manually by adjusting simply the name of the target CSV file and locating it in the right directory.
V2 - will use multithreading to target and process all CSVs placed in the relevant directory, cleanup and outputting an audit result.txt file with the username and date; into the same directory.

V3 - will carry a auto-flagging and approving system based on a historic dataset of all cashouts in the past.
V4 - Refactoring / performance upgrades;


## Usage per verison:

### V1
NOTE:
V1 can only handle each CSV one by one, it cannot handle entire folders full of CSVs.
V1 also has limitations on detecting device sharing, and it does not flag users based on the outcome, it is up to the auditor / liveops team member to determine what the outcomes of the numbers mean.

1. Add your CSV files from Athena into the CSVs folder in the V1 folder; CASHOUT_AUDITING/V1/CSVs
2. Change the filename in the V1/main.py script to the file name of the CSV
3. Run the script, and the outcome will be copied into the clipboard.

Preparation:
- Please use the Query provided within the script to query athena to get the relevant CSV downloaded from Athena;

### V2






## Architecture / specifications

For each cashout the script should eventually provide;
--  A formatted text file containing different types of data
--- Verdict: Approved, Flagged(reasons), Rejected(reasons)
--- Value Source Breakdown: GameEnd%, MatchUP%, tournament wins%, GoalClaims%, MegaSpins%, AdminAddBalance%
--- Key Numbers: Take Per Game, Games to Cashout, Number of Invites, winrate;

--- Collusion: % of money from invited players;
--- Collusion / multiacc: device sharing

Final output should be a JSON file that either...
- Gets sent to the admin backend using admin API and saved onto the relevant cashout index on admin UI
- Or have a parser.py that can be called on the JSON to just pretty-print out the cashout audit logs to be manually used by liveops staff;


### TODO:
1. Complete / export v1 to incorporate into liveops processes
2. Refactor v1 into v2 to handle folder wide tasks and cleanups / archiving;
--> at this point, can work with other engineers to make a server side script that queries Athena automatically once a day, and automatically runs the script on the downloaded CSVs.



### COMPELTED TODOs:
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
