import os
import pandas as pd
import hashlib
import audits


# constants
SCRIPT_PATH = f'{os.path.sep}'.join(__file__.split(f'{os.path.sep}')[:-1]) # path of the current script itself;
EMAIL_HASH_KEY = "****" # please ask for the EMAIL HASH on slack.





# helper functions for email hashing and file maintenance;

def extract_current_username_email(dataframe):
    """
    extract_current_username_email(dataframe):
    Takeas a dataframe, organizes and sorts it and calls various functions in audits.py to extract out the current username and email
    and return the hashed version of the email, along with the username.

    returns;
    - str: username
    - str: email_hash
    """
    dataframe = audits.filter_and_order(dataframe)
    dataframe['time'] = pd.to_datetime(dataframe['time'])
    dataframe.set_index('time', inplace=True)

    cashout_dataframe, ts1, ts2, cashout_value, username, email_address = audits.get_cashout_events(dataframe)
    return username, anonymize_email(email_address)

def anonymize_email(email, secret_key=EMAIL_HASH_KEY):
    """
    anonymize_email(email, secret_key): Anonymize an email using a hash function and a secret key.

    Args:
    - email (str): Email address to be anonymized.
    - secret_key (str): Secret key used for anonymization.

    Returns:
    - str: Anonymized hash representation of the email.
    """
    # Combine the email and the secret key
    combined = str(email) + str(secret_key)
    
    # Generate a SHA256 hash of the combined string
    hash_obj = hashlib.sha256(combined.encode())
    
    # Return the hexadecimal representation of the hash
    return hash_obj.hexdigest()


def check_paypal_sharing(user_key, email_hash, email_hash_df):
    """
    check_paypal_sharing():
    user_key can be both a userid or username. 
    By default username is used, however, in the future to make the system more robust towards 
    edge cases userid should be used to key to account for username changes.
    
    returns:
    - {email_hash: [username1, usernam2] , email_hash2: [username3, username1]}  -- username list for each email hash excludes
    """
    # email hashes are in a list format
    email_hashes = [email_hash]
    # the final returned value will be in a dict format format where email_hash: [list of usernames]
    emailhash_user_dict = {email_hash: [],}

    # first we check whether this username has used any other email hashes in the past, excluding the one passed into the function.
    other_past_email_hashes = email_hash_df.loc[(email_hash_df['username'] == user_key)
                                                &
                                                (email_hash_df['email_hash'] != email_hash), 'email_hash']

    for hash in set(list(other_past_email_hashes)):
        email_hashes.append(hash)
    

    # for each email_hash, check if there has been any users sharing any email;
    for hash in email_hashes:
        shared_users_list = email_hash_df.loc[(email_hash_df['username'] != user_key)
                                               &
                                               (email_hash_df['email_hash'] == hash), "username"]
        

        emailhash_user_dict[hash] = list(set(list(shared_users_list)))


    return emailhash_user_dict


if __name__ == "__main__":
    # section for constructing initial email_hash.csv table containing history of hashed "username, email_hash"" columns
    """
    In order to update this:
     1. execute the following query, download the results and 
     2. put it in the current directory where script is (CASHOTU_AUDITING/V2/) as email_pre_hash.csv:
     3. The hash itself will be shared on slack, please replace it.

/* GENERAL - username-payee table */
/* Manual input variables marked between [brackets] - please input [USERNAME] [DEST_SERVER], remove the brackets after input */

SELECT username, payee
WHERE type = 'CashoutFinish'
AND NOT iserror
ORDER BY time DESC

    UNCOMMENT THE BELOW code ONLY if you have completed the steps above and are ready to refresh the email hashes completely or use a new hash key.
    """
    ## read the CSV into a dataframe
    unhashed_emails_csv_path = SCRIPT_PATH + os.path.sep + "email_pre_hash.csv"
    unhashed_email_df = pd.read_csv(unhashed_emails_csv_path)
    # print(unhashed_email_df)

    ## apply the hashing function to the payee column, and drop the column
    unhashed_email_df['email_hash'] = unhashed_email_df['payee'].apply(anonymize_email)
    hashed_email_df = unhashed_email_df[['username', 'email_hash']]
    print(hashed_email_df)

    ## save the new dataframe into email_hash.xlsx
    email_hash_csv_dest_path = SCRIPT_PATH + os.path.sep + "email_hash.csv"
    hashed_email_df.to_csv(email_hash_csv_dest_path)

