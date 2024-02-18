import os
import sys

if len(sys.argv) != 2:
    print("Usage: python script_name.py <path_to_folder>")
    folder_path = input("Enter path to folder:\n")
else:
    folder_path = sys.argv[1]

# Open the file and read the lines into a list
with open(rf"{folder_path}\Dates.txt", 'r') as file:
    dates = [line.strip().replace('-', '') for line in file.readlines()]

for item in os.listdir(folder_path):
    item = item.split('.')
    if item[0] == "Dates" or item[0] == "Missing_Dates" or item[-1] != "txt":
        continue
    date1 = ''.join(item[0].split('_')[0:3])
    date2 = item[0].split('_')[0]
    if date1 in dates:
        dates.remove(date1)
    elif date2 in dates:
        dates.remove(date2)

print("Missing Dates:")
print(dates)
with open(fr"{folder_path}\Missing_Dates.txt", 'w') as file:
    for date in dates:
        file.write(date + '\n')
print(f"Missing_Dates files created in {folder_path}")
