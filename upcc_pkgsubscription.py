
pkg_services_source="PACKAGESERVICES.csv"

# file format (first row must be header with predefined names as below):
#PACKAGENAME,SERVICENAME
#123456,Default_Limit_Redirect
#123456,1234563
#123456,1234561
#402253,4022532

pkgsubscription=dict()

import csv

with open(pkg_services_source, newline='') as csvfile:
    reader = csv.DictReader(csvfile)
    for row in reader:
        if row['PACKAGENAME'] not in pkgsubscription:
            pkgsubscription[row['PACKAGENAME']]=list()
        pkgsubscription[row['PACKAGENAME']].append(row['SERVICENAME'])

if __name__ == "__main__":
    print ("testing module")
    
