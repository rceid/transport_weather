#!/usr/bin/env python3

import sys
import csv
import os

TOPIC = {
    "batch_header": [],
    "batch_header2": [],
    "batch_header3": [],
    "administrative": [],
    "offense": [],
    "property":[],
    "victim":[],
    "offender": [],
    "arrestee": [],
    "arrest": []
    }


def go(text):
    CSV_FOLDER = "./CSVs"
    os.mkdir(CSV_FOLDER)
    lines = text.splitlines()
    for record in lines: #parsing each record
        cat = record[0:2]
        if cat == "BH":
            parse_bh(record)
        elif cat == "B2":
            parse_bh2(record)
        elif cat == "B3":
            parse_bh3(record)
        elif cat == "01" or "W1":
            parse_admin(record)
        elif cat == "02":
            parse_offense(record)
        elif cat == 
        elif cat ==:
        elif cat ==:
        elif cat ==:
            
        
    offense_file.append([lines[0], 0])
    offense_file.append([lines[1], 2])
    with open(CSV_FOLDER + "/offense-{}.csv".format(YEAR), "w") as file:
        writer = csv.writer(file)
        writer.writerows(offense_file)
    with open(CSV_FOLDER + "/dummy-{}.csv".format(YEAR), "w") as file:
        writer = csv.writer(file)
        writer.writerows(offense_file)
    
    
    

if __name__ == "__main__":
    YEAR = sys.argv[1]
    text = sys.stdin.read()
    go(text)
    

