import os

#Path to date file
date_file = r""

#Path to Folder
folder_path = r""

# Open the file and read the lines into a list
with open(date_file, 'r') as file:
    dates = [line.strip() for line in file.readlines()]

for item in os.listdir(folder_path):
    print(item)
