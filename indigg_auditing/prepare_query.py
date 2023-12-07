import pyperclip
import pandas as pd

df = pd.read_csv("provided_users.csv", skiprows=1)
query_usernames = "(" + ", ".join(f"'{username}'" for username in df['Username'].to_list()) + ")"
pyperclip.copy(query_usernames)

