#!/usr/bin/env python3

import sys
import csv
import os
import fbi_codes as fc
import time
import logging

TOPIC = {
    "BH": [],
    "B2": [],
    "B3": [],
    "administrative": [], #two codes 01 and W1
    "02": [], #offense
    #"property":[], #two groups
    "04":[],
    "05": []
    #"arrestee": [], 
    #"07": [] 
    }


def parse(text):
    CSV_FOLDER = "./CSVs"
    start = time.perf_counter()
    lines = text.splitlines()
    n = len(lines)
    logging.info("\n{} minutes to tokenize {} lines".format((time.perf_counter() - start)/60, n))
    parse = time.perf_counter()
    for idx, record in enumerate(lines): #parsing each record
        if idx % 1_000_000 == 0:
            logging.info("line {} @ {}".format(idx, time.ctime(time.time())))
        cat = record[0:2]
        if cat == "BH":
            parse_bh(record, cat)
        elif cat == "B2" or cat == "B3":
            parse_bh_other(record, cat)
        elif cat == "01" or cat == "W1":
            parse_admin(record)
        elif cat == "02":
            parse_offense(record, cat)
        elif cat == "04":
            parse_victim(record, cat)
        elif cat == "05":
            parse_offender(record, cat)
#         elif cat == "03" or cat == "W3":
#             parse_property(record)
#         elif cat == "06" or cat == "W6":
#             parse_arrestee(record)
#         elif cat == "07":
#             parse_arrrest(record, cat)

    logging.info("\n{} minutes to parse all text".format((parse - time.perf_counter())/60))
    to_csv = time.perf_counter()
    for topic, rows in TOPIC.items(): #writing all records to csv
        if rows:
            with open(CSV_FOLDER + "/{}-{}.csv".format(topic, YEAR), "w") as file:
                writer = csv.writer(file)
                writer.writerows(rows)
    end = time.perf_counter()
    logging.info("\n{} minutes to write text to csv \n{} lines per minute".\
                 format((end - to_csv)/60, n/((end - start)/ 60)))
    logging.info("\nCSVs written, script complete: {}".\
                 format(time.ctime(time.time())))

    
def parse_bh(rec, cat):
    st = get_code(rec, 4,6)
    ori = get_code(rec, 4, 13)
    i_no = get_code(rec, 13, 25)
    nbr_yr = get_code(rec, 33, 37)
    nbr_mo = get_code(rec, 37, 39)
    city = get_code(rec, 41, 71)
    div = get_letter(rec, 75)
    div = fc.division.get(div, "")
    reg = get_letter(rec, 76)
    reg = fc.region.get(reg, "")
    loc = get_letter(rec, 77)
    loc = fc.agency.get(loc, "")
    city_flag = get_letter(rec, 78)
    city_flag = fc.y_n.get(city_flag, "")
    office = get_code(rec, 88, 92)
    nibrs_flag= get_letter(rec, 96)
    nibrs_flag = fc.flag.get(nibrs_flag, "")
    pop = get_code(rec, 105, 114)
    pop = get_num(pop)
    cols = [st, ori, i_no, nbr_yr, nbr_mo, city, div, reg, loc, city_flag,\
            office, nibrs_flag, pop]
    TOPIC[cat].append(cols)
    
def parse_bh_other(rec, cat):
    st = get_code(rec, 4,6)
    ori = get_code(rec, 4, 13)
    pop = get_code(rec, 25, 34)
    pop = get_num(pop)
    cols = [st, ori, pop]
    TOPIC[cat].append(cols)

def parse_admin(rec):
    cat = "administrative"
    st = get_code(rec, 4,6)
    ori = get_code(rec, 4, 13)
    i_no = get_code(rec, 13, 25)
    i_yr = get_code(rec, 25, 29)
    i_mo = get_code(rec, 29, 31)
    i_day = get_code(rec, 31, 33)
    hour = get_code(rec, 34, 36)
    hour = get_num(hour)
    num_off = get_code(rec, 36, 38)
    num_off = get_num(num_off)
    num_vic = get_code(rec, 38, 41)
    num_vic = get_num(num_vic)
    num_offender = get_code(rec, 41, 43)
    num_offender = get_num(num_offender)
    num_arrestee = get_code(rec, 43, 45)
    num_arrestee = get_num(num_arrestee)
    off = get_code(rec, 58, 61)
    off = fc.offenses.get(off, "")
    cols = [st, ori, i_no, i_yr, i_mo, i_day, hour, num_off, num_vic,\
           num_offender, num_arrestee, off]
    TOPIC[cat].append(cols)
    
def parse_offense(rec, cat):
    st = get_code(rec, 4,6)
    ori = get_code(rec, 4, 13)
    i_no = get_code(rec, 13, 25)
    i_yr = get_code(rec, 25, 29)
    i_mo = get_code(rec, 29, 31)
    i_day = get_code(rec, 31, 33)
    ucr = get_code(rec, 33, 36)
    ucr = fc.offenses.get(ucr, "")
    att_comp = get_letter(rec, 36)
    att_comp = fc.attempt_complete.get(rec, "")
    loc_type = get_code(rec, 40, 42)
    loc_type = fc.location.get(loc_type, "")
    weap = get_code(rec, 48, 50)
    weap = fc.weapon.get(weap, "")
    auto = get_letter(rec, 50)
    auto = fc.automatic.get(auto,"")
    bias_ = get_code(rec, 57, 59)
    bias_grp = fc.bias_group.get(bias_, "")
    bias_ = fc.bias.get(bias_, "")
    cols = [st, ori, i_no, i_yr, i_mo, i_day, ucr, att_comp, loc_type,\
            weap, auto, bias_, bias_grp]
    TOPIC[cat].append(cols)
        
def parse_victim(rec, cat):
    st = get_code(rec, 4,6)
    ori = get_code(rec, 4, 13)
    i_no = get_code(rec, 13, 25)
    i_yr = get_code(rec, 25, 29)
    i_mo = get_code(rec, 29, 31)
    i_day = get_code(rec, 31, 33)
    v_id = get_code(rec, 33, 36)
    ucr = get_code(rec, 36, 39)
    ucr = fc.offenses.get(ucr, "")
    v_type = get_letter(rec, 66)
    v_type = fc.victim_type.get(v_type, "")
    age_ = get_code(rec, 67, 69)
    age_ = get_num(fc.age.get((age_, "")))
    sex_ = get_letter(rec, 69)
    sex_ = fc.sex.get(sex_, "")
    race_ = get_letter(rec, 70)
    race_ = fc.race.get(race_, "")
    ethni = get_letter(rec, 71)
    ethni = fc.race.get(ethni, "")
    a_circum = get_code(rec, 73, 75)
    a_grp = fc.circumstance_group.get(a_circum, "")
    a_circum = fc.circumstance.get(a_circum, "")
    cols = [st, ori, i_no, i_yr, i_mo, i_day, v_id, ucr, v_type,\
            age_, sex_, race_, ethni, a_circum, a_grp]
    TOPIC[cat].append(cols)
    
def parse_offender(rec, cat):
    st = get_code(rec, 4,6)
    ori = get_code(rec, 4, 13)
    i_no = get_code(rec, 13, 25)
    i_yr = get_code(rec, 25, 29)
    i_mo = get_code(rec, 29, 31)
    i_day = get_code(rec, 31, 33)
    o_id = get_code(rec, 33, 35)
    age_ = get_code(rec, 35, 37)
    age_ = get_num(fc.age.get((age_, "")))
    sex_ = get_letter(rec, 37)
    sex_ = fc.sex.get(sex_, "")
    race_ = get_letter(rec, 38)
    race_ = fc.race.get(race_, "")
    cols = [st, ori, i_no, i_yr, i_mo, i_day, o_id, age_, sex_, race_]
    TOPIC[cat].append(cols)
    
def get_code(record, lb, ub):
    try:
        code = record[lb:ub]
        return code
    except:
        return ""
    
def get_letter(record, idx):
    try:
        code = record[idx]
        return code
    except:
        return ""
    
def get_num(num):
    try:
        num = int(num)
        return num
    except:
        return ""
    
# def parse_property(rec):
#     cat = "property"
# def parse_arrestee(rec):
#     cat = "arrestee"
# def parse_arrrest(record, cat):


if __name__ == "__main__":
    YEAR = sys.argv[1]
    logging.basicConfig(filename="./logs/{}.txt".format(YEAR), level=logging.INFO)
    logging.info("\nBeginning: {}".format(time.ctime(time.time())))  
    text = sys.stdin.read()
    parse(text)
