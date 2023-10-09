import audits
import datetime
import pandas as pd
import os
from threading import Thread, Lock
from pathlib import Path
import shutil


"""
V2 Notes:

I must do the two audits separately, and construct them separately.

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
email_hash_df = pd.read_csv(audits.SCRIPT_PATH + os.path.sep + "email_hash.csv")
response_lock = Lock()



# define the main function to audit the code, in a mulitthreaded manner
def audit_csv(csv_path):
    """
    audit_csv(csv_path) - takes a CSV path as a parameter, and runs the 
    """
    # access the global variable
    global response_string
    global email_hash_df

    # process the audit
    df = pd.read_csv(csv_path, low_memory=False)
    result = audits.audit(df)

    # with the lock, edit the response string
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
