import psycopg2
import os
import sys
import datetime
import json

## See SQLTesting.py for examples of how to connect to the database and execute queries and transactions
##
conn = psycopg2.connect("dbname=elections user=vagrant")
cur = conn.cursor()

def processJSON(j):
    with open(j).read() as f:
        json_vales = f.read()
    conn.commit()
    myquery = """INSERT INTO pres_county_returns(json_vales) VALUES(%s,%s,%s,%s)"""

    cur.execute(myquery, (json_vales))
    conn.commit()
    print(j)

if len(sys.argv) < 2:
    print("You must pass in a JSON file with the data to be inserted in the database")
else:
    with open(sys.argv[1], 'r') as f:
        j = json.load(f)
        processJSON(j)

