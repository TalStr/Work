import os

#Path to Folder
folder_path = input("Enter path to folder:\n")

# Open the file and read the lines into a list
with open(rf"{folder_path}\Dates.txt", 'r') as file:
    dates = [line.strip().replace('-', '') for line in file.readlines()]

for item in os.listdir(folder_path):
    item = item.split('.')
    if item[0] == "Dates" or item[0] == "Missing_Dates" or item[-1] != "txt":
        continue
    date = ''.join(item[0].split('_')[0:3])
    if date in dates:
        dates.remove(date)

print("Missing Dates:")
print(dates)
with open(fr"{folder_path}\Missing_Dates.txt", 'w') as file:
    for date in dates:
        file.write(date + '\n')
print(f"Missing_Dates files created in {folder_path}")
