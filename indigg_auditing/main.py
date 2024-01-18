import pyperclip
import pandas as pd

# preparing the queried usernames
provided_df = pd.read_csv("provided_users.csv", header=1)
query_usernames = "(" + ", ".join(f"'{username}'" for username in provided_df['Username'].to_list()) + ")"
pyperclip.copy(query_usernames)
print(len(provided_df['Username'].to_list()))
provided_df = provided_df['Username']


# loading the audited results
audited_df = pd.read_csv("audit_result.csv")
audited_df = audited_df.rename(columns={'username':"Username"})
print(audited_df)


result = pd.merge(provided_df, audited_df, on='Username', how='left')
def check_if_completed(n_games):
    if n_games >= 25:
        return True
    elif n_games >= 0:
        return False
    else:
        return None


result['completed'] = result['event_count'].apply(check_if_completed)
# print(result)
result.to_csv("validated_data.csv")
invalid_accounts = result.loc[(result['completed']!=True)]
invalid_accounts.to_csv("invalid_accounts.csv")

# print(invalid_accounts)

# Get all valid users:
print(result.loc[result['completed'].isna()])


## mapping inviters
inviter_df = pd.read_csv("inviters.csv")
inviter_mapped_df = pd.merge(result, inviter_df, on='Username', how='left')
inviter_mapped_df.to_csv("inviter_mapped.csv")
unique_inviters = inviter_mapped_df['inviterusername'].unique()

uinv_df = pd.DataFrame({'unqiue_inviters': unique_inviters})
uinv_df.to_csv("unique_inviters.csv")

