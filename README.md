
# Overview
Cashout auditing scripts intended to automate and streamline this liveops process.

V1 - is usable manually by adjusting simply the name of the target CSV file and locating it in the right directory.
V2 - will use multithreading to target and process all CSVs placed in the relevant directory, cleanup and outputting an audit result.txt file with the username and date; into the same directory.



V3 - will carry a auto-flagging and approving system based on a historic dataset of all cashouts in the past.


## Usage per verison:


### V3

#### USAGE:
Instructions are the same as V2 - however, the script will now infer based on the audit data to raise flags on data points and approve, flag or even reject cashouts.
Eventually, this can be integrated with the TallyUP Admin backend API, or simply digested by the server and pushed onto the AdminUI while changing the states based on each result.

Therefore, this version outputs a JSON version and a txt version as outputs (with configurations available depending on the sys.argv of the calls)



### V2

#### USAGE:
1. Download all the relevant CSV files using the audit query on Athena and putting them into the V2/CSVs folder. (Alternatively, have a server side script that queries, downloads and saves CSVs into the CSVs folder once a day, so that we can automate the querying + downloading CSVs part.)

2. Run main.py, this will output a new txt file in V2/results/, with the date of the audit, containing all of the relevant audit notes in V2/results/audits_YYYY-MM-DD.txt.
3. After the audits are complete, main.py cleans up the CSVs and moves them into a sub directory: /V2/CSVs/audited_csvs/
4. LiveOps staff may then use the audit notes to infer a lot about each cashout with prepared audit notes.

TODO:
- Support JSON output mode (which the daily serverside script can then upload into the admin UI once per day for each relevant cashout id)


#### NOTE:
The process of downloading all of the CSV files should be done as a server side script that runs once a day, that queries and downloads all of the relevant CSV files for the cashed out players.










### V1
#### USAGE:
V1 can only manually audit each CSV one by one, it cannot handle entire folders full of CSVs.
It is intended to be a tool to accelerate and streamline the workflow and process for liveops staff.
V1 also has limitations on detecting device sharing, and it does not flag users based on the outcome, it is up to the auditor / liveops team member to determine what the outcomes of the numbers mean.

Workflow in using V1 to run a basic audit on the cashout:

1. Copy and past the V1/current_query.txt - changing the username / userid and other variables to match the cashout to be audited.
2. Download the query result csv file, rename it to the relevant format (suggested: username_date) and add it to V1/CSVs
3. Change the "name" variable in the V1/main.py script to the file name of the newly downloaded CSV - currently this is line 29, by default set to:
name = "wrickmon_9192023" # this is the name of the file and what you want to change in V1

4. Run the script, and the outcome will be copied into the clipboard. You may paste this onto the audit note of the cashouts tab in admin UI.


Preparation:
Your query file is located in current_query.txt for each version. Paste it into Athena and change the key to get the dataframe for the user that has cashed out.

#### NOTE:
The audit will only work properly if the user's full gameplay data is in athena.
This means, we can begin auditing from the T-1 day of today;
So if we are at 9/15/2023, we can audit up to 9/14/2023.













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
