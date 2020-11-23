#!/usr/bin/env python3

import sys
import csv
import os
import fbi_codes

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
        elif cat == "03":
            parse_property(record)
        elif cat == "04":
            parse_victim(record)
        elif cat == "05":
            parse_offender(record)
        elif cat == "06":
            parse_arrestee(record)
        elif cat == "07":
            parse_arrrest(record)
    for topic, rows in TOPIC: #writing all records to csv
        if rows:
            with open(CSV_FOLDER + "/{}-{}.csv".format(topic, YEAR), "w") as file:
                writer = csv.writer(file)
                writer.writerows(rows)

def parse_bh(record):
    return None
        
def parse_bh2(record):
    return None
        
def parse_bh3(record):
    return None
        
def parse_admin(record):
    return None
        
def parse_offense(record):
    return None
        
def parse_property(record):
    return None
        
def parse_victim(record):
    return None
        
def parse_offender(record):
    return None
        
def parse_arrestee(record):
    return None

def parse_arrrest(record):
    return None
    

    
    

if __name__ == "__main__":
    YEAR = sys.argv[1]
    text = sys.stdin.read()
    go(text)
    

