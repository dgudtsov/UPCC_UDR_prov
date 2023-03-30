
servicequota_source="SERVICEQUOTA.csv"

# file format (first row must be header with predefined names as below):
#SERVICENAME,QUOTANAME
#1,1-DATA_D_Quota
#100001,100001-DATA_D_Quota
#100002,100002-DATA_D_Quota
#100003,100003-DATA_D_Quota

servicequota=dict()

import csv

# with open(servicequota_source, newline='') as csvfile:
#     reader = csv.DictReader(csvfile)
#     for row in reader:
#
#         if row['SERVICENAME'] not in servicequota:
#             servicequota[row['SERVICENAME']]=list()
#         servicequota[row['SERVICENAME']].append(row['QUOTANAME'])

if __name__ == "__main__":
    print ("testing module")
    
