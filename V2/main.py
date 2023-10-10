import audits
import datetime
import pandas as pd
import os
from threading import Thread, Lock
from pathlib import Path
import shutil
import email_audits




"""
V2 Notes:

/// alternate approach (best of both worlds)
1 - read the user dataframes in main.py;
2 - extract run the email audits separately, hashing and audits fully in main.py but in a separate thread using a separate lock.
3 - then, pass the results of the email audits data INTO the audits.py so that it can go through the flaggign and string constrution process.

THIS STEP IS DONE;
Now, I need to renew the audits and get the right columns for the given player.

"""


# define the target folder for processing:
csvs_path = f"{os.path.sep}".join(__file__.split(f"{os.path.sep}")[:-1]) + os.path.sep + "CSVs"
csv_filenames = os.listdir(csvs_path)
csv_paths = []
for filename in csv_filenames:
    file_path = csvs_path+os.path.sep+filename
    if os.path.isfile(file_path):
        if filename.split('.')[-1] == "csv":
            csv_paths.append(file_path)
        else:
            pass
    else:
        pass


# define the response string, and the lock:
time = datetime.datetime.now().strftime('%Y/%m/%d %Y:%M %p')
today = datetime.datetime.now().strftime('%Y-%m-%d')
response_string = f"AUDIT DATE: {time}\n---------------\n"
response_lock = Lock()

email_hash_df = pd.read_csv(audits.SCRIPT_PATH + os.path.sep + "email_hash.csv")
email_hash_df_lock = Lock()




# define the main function to audit the code, in a mulitthreaded manner
def audit_csv(csv_path):
    """
    audit_csv(csv_path) - takes a CSV path as a parameter, and runs the 
    """
    # access the global variable
    global response_string
    global email_hash_df

    # read the dataframe
    df = pd.read_csv(csv_path, low_memory=False)


    # get the email_hash_df_lock() before reading and handling the email_hash dataframe
    with email_hash_df_lock:
        # call email_audits and extract the username and email from the dataframe by using the audits.py functions within email_audits.py
        username, email_hash = email_audits.extract_current_username_email(df)

        # concatenate to the email_hash_df so that it may be used;
        username_emailhash_dict = {
            "username": [username],
            "email_hash": [email_hash]
        }
        new_entries = pd.DataFrame(username_emailhash_dict)
        concatenated_df = pd.concat(new_entries, email_hash_df)
        email_hash_df = concatenated_df

        # then search against it with the existing email dataframe
        email_audit_result = email_audits.check_paypal_sharing(username, email_hash, email_hash_df)

    # process the audit with relevant dataframe, and email audits;
    result = audits.audit(df, email_audit_result)
    

    # with the lock, edit the response string for the whole txt file / folder
    with response_lock:
        response_string += result
    
    print(f"{csv_path} audited!")


# create the threads for each
threads = []


for csv in csv_paths:
    thread = Thread(target=audit_csv, args=(csv,))
    threads.append(thread)
    thread.start()

for thread in threads:
    thread.join()

# output the text file:
results_directory = os.path.sep.join(__file__.split(os.path.sep)[:-1]) + os.path.sep + "results" + os.path.sep
result_path = results_directory + f"audits_{today}.txt"
with open(result_path, 'w') as wf:
    wf.write(response_string)

# cleanup for CSVs folder;
for orig_path in csv_paths:
    
    filename = orig_path.split(os.path.sep)[-1]
    dest_path = csvs_path+os.path.sep+"audited_CSVs"+os.path.sep+filename
    shutil.move(orig_path, dest_path)


# update the email_hash.csv file itself with new dataframe;



