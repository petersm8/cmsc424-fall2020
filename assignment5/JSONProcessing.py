import json
import sys

import psycopg2

## See SQLTesting.py for examples of how to connect to the database and execute queries and transactions
##
conn = psycopg2.connect("dbname=elections user=vagrant")
cur = conn.cursor()


def processJSON(j):
    statecode = j['statecode']
    year = j['year']
    query = "select statecode from states where statecode ='" + str(statecode) + "'"
    cur.execute(query)
    ans = cur.fetchall()
    if len(ans) == 0:
        print('Invalid statecode ' + statecode + ': updates rejected.')
        return

    for pre in range(0, len(j['presidential'])):
        votes = j['presidential'][pre]['votes']
        countyname = j['presidential'][pre]['countyname']

        for vot in range(0, len(votes)):
            candidatename = votes[vot]['candidatename']
            partyname = votes[vot]['partyname']
            candidatevotes = votes[vot]['candidatevotes']
            query = "select * from pres_county_returns where countyname = %s and statecode =%s and candidatename =%s and year= %s"
            cur.execute(query, (countyname, statecode, candidatename, year))
            ans2 = cur.fetchall()
            if len(ans2) == 0:
                myquery = "INSERT INTO pres_county_returns VALUES(%s,%s,%s,%s,%s,%s)"
                cur.execute(myquery, (year, statecode, countyname, candidatename, partyname, candidatevotes))

                conn.commit()
            else:
                myquery = "update pres_county_returns set candidatevotes =%s where candidatename = %s and countyname = %s and statecode = %s and year = %s"
                cur.execute(myquery, (candidatevotes, candidatename, countyname, statecode, year))
                conn.commit()


if len(sys.argv) < 2:
    print("You must pass in a JSON file with the data to be inserted in the database")
else:
    with open(sys.argv[1], 'r') as f:
        j = json.load(f)
        processJSON(j)
