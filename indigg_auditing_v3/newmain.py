import pyperclip
import pandas as pd
from collections import Counter

# read the CSV provided by IndiGG
provided_df = pd.read_csv("provided_users.csv", header=1)


# get the list of initial usernames
## remove all of the "'" in the usernames, which is an invalid char and makes issues for the audit
stripped_usernames = [username.replace("'", "") for username in provided_df['Username'].to_list()] 

# Pre-Filter 1: take out all duplicates
invalid_usernames = ""
username_counts = Counter(stripped_usernames)
duplicates = [user for user, count in username_counts.items() if count > 1]
invalid_usernames += "\n".join(duplicates)
with open ('duplicate_usernames.txt', 'w') as duplicate_wf:
    duplicate_wf.write(invalid_usernames) 
    # currently returns an empty doc, meaning no duplicate entries from IndiGG. If any, it will show up in this doc



# Get the unique usernames and construct the query
unique_usernames_set = [user for user, count in username_counts.items() if count == 1]
query_username_full_list = "(" + ", ".join(f"'{username}'" for username in unique_usernames_set) + ")" # parse the unique username set into correct format ('user1', 'user2') for athena


# Filter 2: take out all users that have registered before we even launched in India (and therefore +1 month before IndiGG quest even begun)
filter_2_query = f"""
/* IGG Filter 2: take out all users that have registered before we launched in India */
/* This is achieved by getting a list of all users that have incurred an event BEFORE the date we launched in India */
/* Such that if ('SAHILINDIGG1', 'Vivek_Indigg', 'Awoo') were entered as list of users, only Awoo would have event counts, thus eliminating Awoo */

SELECT username, COUNT(*) as event_count
FROM "productiondbcatalogs-iv2e77bcwp31".production_events_etl
WHERE username IN {query_username_full_list}
AND time < DATE '2023-10-08'
GROUP BY username;
"""
with open ('filter_2_query.txt', 'w') as f2_wf:
    f2_wf.write(filter_2_query)

# save the results csv as filter_2_results.csv locally.
filter_2_results = pd.read_csv("filter_2_results.csv")
users_pre_india_launch = filter_2_results['username'].to_list()
# print(users_pre_india_launch) # filter_2_results.csv contains all of such users above to be filtered.



# Filter 3: find all of the users that have won 25 games.
## construct a new list, filtering out the results from above for players that registered pre India Launch
filtered_usernames_pre_india = [user for user in unique_usernames_set if user not in users_pre_india_launch]
filtered_usernames_formatted_f2 = "(" + ", ".join(f"'{username}'" for username in filtered_usernames_pre_india) + ")" # parsing again
filter_3_query = f"""
/* IGG Filter 3: now simply find all of the games that they've won */

SELECT username, COUNT(*) as event_count
FROM "productiondbcatalogs-iv2e77bcwp31".production_events_etl
WHERE username IN {filtered_usernames_formatted_f2}
AND (type = 'GameEnd' OR type = 'AcknowledgeResult')
AND istransaction
AND (result = 'win' OR reason = 'win')
GROUP BY username
ORDER BY event_count;
"""

with open ('filter_3_query.txt', 'w') as f3_wf:
    f3_wf.write(filter_3_query)


# Filter 4: construct the final gameplay results
# Once you have the query results -> save it as gameplay_audit_result.csv, read and process it.
audited_df = pd.read_csv("gameplay_audit_result.csv")
audited_df = audited_df.rename(columns={'username':"Username"}) # rename the result column for matching column names with provided CSV from IndiGG
provided_df = provided_df['Username'] # just get the username column only

# construct the final results based on the audited data
final_result = pd.merge(provided_df, audited_df, on='Username', how='left')
def check_if_completed(n_games):
    if n_games >= 25:
        return True
    elif n_games >= 0:
        return False
    else:
        return None

final_result['completed'] = final_result['event_count'].apply(check_if_completed)
final_result.to_csv("validated_data.csv")

"""
Outcome:
validated_data.csv <- contains the full results
"""


