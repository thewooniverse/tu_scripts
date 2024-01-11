
# importing moduels
import datetime
import os
import pandas as pd



# extract the CSV file named "quest_completers.csv" and divide them into chunks
df = pd.read_csv("quest_completers.csv", header=1)
usernames = df['Username'].to_list()

def divide_into_chunks(lst, chunk_size=500):
    # For each index in range 0 to len(lst), stepping by chunk_size
    for i in range(0, len(lst), chunk_size):
        # Yield a chunk of the list
        yield lst[i:i + chunk_size]

chunks = list(divide_into_chunks(usernames))


# export each chunk into the query:
export_string = "The queries have been divided into chunks as the n of usernames has become too long. Please use them one by one\n"
export_string += f"Once you've run all {len(chunks)} queries, please put all the results into the results directory.\n"
divider = "/* ------------------------- */"
export_string += '\n\n\n\n\n'

for (idx,chunk) in enumerate(chunks):
    query_usernames = "(" + ", ".join(f"'{username}'" for username in chunk) + ")"
    export_string += f"This is query number {idx+1}\n"
    query = f"""/* IndiGG: Quest 1 Validation Query */
SELECT username, COUNT(*) as event_count
FROM "productiondbcatalogs-iv2e77bcwp31".production_events_etl
WHERE username IN {query_usernames}
AND (type = 'GameEnd' OR type = 'AcknowledgeResult')
AND istransaction
AND (result = 'win' OR reason = 'win')
GROUP BY username
ORDER BY event_count;
"""
    export_string += "\n"
    export_string += query
    export_string += "\n\n\n"


# export
script_path = os.path.abspath(__file__)
directory_path = os.path.dirname(script_path)

with open(os.path.join(directory_path, "query.txt"), 'w') as file:
    file.write(export_string)



