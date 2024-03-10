usernames = """fly42
AMgudito23
"""


invalid_names = usernames.split('\n')
query_usernames = "(" + ", ".join(f"'{username}'" for username in invalid_names) + ")"