import pyperclip

s = """
IndiGG
AnimeshGhosh
braveboy
nazmulx112
bimlesh07
ayan09
alamin2023
Ajit
Nimain
mannu
SuroCR7
mannu
Sayan
mannu
Johnny_Sins
alamin2023
SAHILGAMING
alamin2023
anuragM
poornima
"""
ls = s.split('\n')[1:-1]
print(ls)
result = "(" + ", ".join(f"'{username}'" for username in ls) + ")"
pyperclip.copy(result)

