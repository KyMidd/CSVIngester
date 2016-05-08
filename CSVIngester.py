# Ingests CSVs and creates new column for site code
#  Author: Kyler Middleton

# Future improvements: Don't use intermediary CSV files, import raw data directly into array, combine arrays

# Imports
import csv
from collections import OrderedDict
import pandas as pd

# Sites list input and build dictionary 1
sitesInput = csv.DictReader(open('rawData/SiteList.csv', encoding='latin-1'))
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
    writer.writerow(['Site Code','Node','Interface1','Carrier','Speed1'])
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
        writer = csv.writer(outputFile3, quoting=csv.QUOTE_MINIMAL)
        writer.writerow([siteCode,node,interface2,speed2])

# Organized data for combination
inputA = pd.read_csv("processedData/data1.csv")
inputB = pd.read_csv("processedData/data2.csv")
inputC = pd.read_csv("processedData/data3.csv")

# Merge
merged = inputA.merge(inputB, on='Site Code', how='inner')
merged = merged.merge(inputC, on='Site Code', how='inner')
merged.to_csv("output.csv", index=False)

# Profit

'''
At the end, we want to see this:
Site Code, Site Name, Type of Site (Category, like Sales, Office, etc.), Internet POP, bandwidth,
type of circuit (MPLS or iNet-based), number of users
'''
