import os
import pandas as pd
import hashlib
import audits


# constants
SCRIPT_PATH = f'{os.path.sep}'.join(__file__.split(f'{os.path.sep}')[:-1]) # path of the current script itself;
EMAIL_HASH_KEY = "3113"





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
    email_hashes = [email_hash]
    emailhash_user_dict = {email_hash: [],}


    # first we check whether this username has used any other email hashes in the past, excluding the one passed into the function.
    other_past_email_hashes = email_hash_df.loc[(email_hash_df['username'] == user_key)
                                                &
                                                (email_hash_df['email_hash'] != email_hash), 'email_hash']
    email_hashes.append(list(other_past_email_hashes))


    # for each email_hash, check if there has been any users sharing any email;
    for hash in email_hashes:
        shared_users_list = email_hash_df.loc[(email_hash_df['username'] != user_key)
                                               &
                                               (email_hash_df['email_hash'] == hash), "username"]
        
        emailhash_user_dict[hash] = list(shared_users_list)
    
    
    return emailhash_user_dict


