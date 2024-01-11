1. Download the CSV file in the directory below:
https://docs.google.com/spreadsheets/d/1g3M5DHq59Twf91VCCvsa15n-02Qb3WRiuaVD54AyT3Y/edit?pli=1#gid=1425344565

2. rename the directory to quest_completers.csv and place it into /igg_audit_v2/
3. run generate_queries.py
4. run all of the queries generated in query.txt on athena
5. place all of the result .csv into /igg_audit_v2/results/
6. run validate_date.py
7. all of your validated data should be placed into /igg_audit_v2/results/dd-mm-yyyy-hh-mm/


