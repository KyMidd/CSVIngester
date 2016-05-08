# Ingests CSVs and creates new column for site code
#  Author: Kyler Middleton

# Future improvements: Don't use intermediary CSV files, import raw data directly into array, combine arrays

# What we want to see at output:
# Site Code, Site Name, Type of Site (Category, like Sales, Office, etc.), Internet POP, bandwidth,
#  type of circuit (MPLS or iNet-based), number of users

# Imports
import csv
from collections import OrderedDict

# Sites list input and build dictionary 1
sitesInput = csv.DictReader(open('rawData/SiteList.csv', encoding='windows-1250'))
with open('processedData/data1.csv', 'w', encoding='utf8', newline='') as outputFile1:
    writer = csv.writer(outputFile1, quoting=csv.QUOTE_MINIMAL)
    writer.writerow(['Site Code','Site Name','Site Type'])
    for row in sitesInput:
        siteCode = row["CCID / Location"]
        siteCode = siteCode.replace(" ","")
        siteCode = siteCode.upper()
        siteName = row["Name"]
        siteType = row["Location type"]
        writer = csv.writer(outputFile1, quoting=csv.QUOTE_MINIMAL)
        writer.writerow([siteCode,siteName,siteType])

# Bandwidth list input and build dictionary 2
bandwidthInput = csv.DictReader(open('rawData/WAN_Interface_Bandwidth.csv', encoding='windows-1250'))
with open('processedData/data2.csv', 'w', encoding='utf8', newline='') as outputFile2:
    writer = csv.writer(outputFile2, quoting=csv.QUOTE_MINIMAL)
    writer.writerow(['Site Code','Node','Interface1','Carrier','Speed1','Site Name'])
    for row in bandwidthInput:
        node = row["Node"]
        siteCode = node[:4]
        siteCode = siteCode.upper()
        interface1 = row["Interface"]
        carrier = row["CarrierName"]
        speed1 = row["Speed"]
        writer = csv.writer(outputFile2, quoting=csv.QUOTE_MINIMAL)
        writer.writerow([siteCode,node,interface1,carrier,speed1])

# Circuits list input and build dictionary 3
circuitInput = csv.DictReader(open('rawData/95thPercentileCircuits.csv', encoding='windows-1250'))
with open('processedData/data3.csv', 'w', encoding='utf8', newline='') as outputFile3:
    writer = csv.writer(outputFile3, quoting=csv.QUOTE_MINIMAL)
    writer.writerow(['Site Code','Node','Interface2','Speed2'])
    for row in circuitInput:
        siteCode = row["site code "]
        siteCode = siteCode.upper()
        node = row["Node"]
        interface2 = row["Interface"]
        speed2 = row["Speed"]
        #print(siteCode,node,interface2,speed2)
        writer = csv.writer(outputFile3, quoting=csv.QUOTE_MINIMAL)
        writer.writerow([siteCode,node,interface2,speed2])

# Use pandas library to combine data
from conda import pandas as pd

# Organized data for input
inputA = pd.read_csv("processedData/data1.csv")
inputB = pd.read_csv("processedData/data2.csv")
inputC = pd.read_csv("processedData/data3.csv")

# Remove headers after read into memory
inputB = inputB.dropna(axis=1)
inputC = inputC.dropna(axis=1)

# Merge
merged = inputA.merge(inputB, on='Site Code')
merged = merged.merge(inputC, on='Site Code')
merged.to_csv("output.csv", index=False)

#inputs = ["data/processed/data1.csv", "data/processed/data2.csv", "data/processed/data3.csv"]


"""
# Define fieldnames array
fieldnames = []

# Begin looping through inputs for column headers
for filename in inputs:
    with open(filename, "rt", encoding='utf8', newline="") as f:
        reader = csv.reader(f)
        headers = next(reader)
        for h in headers:
            if h not in fieldnames:
                fieldnames.append(h)

# Loop through rest of data and sort into proper fields
with open("data/out.csv", "w", encoding='utf8', newline="") as f:
    writer = csv.DictWriter(f, fieldnames, quoting=csv.QUOTE_MINIMAL)
    for filename in inputs:
        with open(filename, "r", encoding='utf8', newline="") as f:
            reader=csv.DictReader(f)
            for line in reader:
                writer.writerow(line)
"""

"""

# Open dictionary files, import data into arrays
with open('data3.csv', 'rt', encoding='utf8') as f:
    r = csv.reader(f)
    dict3 = {row[0]: row[1:] for row in r}
with open('data2.csv', 'rt', encoding='utf8') as f:
   r = csv.reader(f)
    dict2 = {row[0]: row[1:] for row in r}
with open('data1.csv', 'rt', encoding='utf8') as f:
    r = csv.reader(f)
    dict1 = OrderedDict((row[0], row[1:]) for row in r)

# Combine dictionaries using first column (siteCode) as key
combinedData = OrderedDict()
for d in (dict1, dict2, dict3):
    for key, value in d.items():
        combinedData.setdefault(key, []).extend(value)

with open('WANOutput.csv', 'wt', encoding='utf8') as f:
    w = csv.writer(f)
    for key, value in combinedData.items():
        w.writerow([key] + value)

# Create final output file with headers, then populate it
with open('WANOutput.csv', 'w', encoding='utf8', newline='') as outputFile:
    writer = csv.writer(outputFile, quoting=csv.QUOTE_MINIMAL)
    writer.writerow(['Site Code','Site Name','Site Type','Bandwidth','Circuit Type','Number of Users'])
    for row in combinedData:
        writer = csv.writer(outputFile, quoting=csv.QUOTE_MINIMAL)
        writer.writerow([siteCode,siteName,siteType])

# Site Code, Site Name, Type of Site (Category, like Sales, Office, etc.), Internet POP, bandwidth,
#  type of circuit (MPLS or iNet-based), number of users

""" 
