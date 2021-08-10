# -*- coding: utf-8 -*-
"""
Created on Mon Aug 10 12:38:36 2020

@author: Sureshkumar D
"""

import json
import csv
import requests
def flatten_json(y): 
    out = {} 
  
    def flatten(x, name =''): 
          
        # If the Nested key-value  
        # pair is of dict type 
        if type(x) is dict: 
              
            for a in x: 
                flatten(x[a], name + a + '_') 
                  
        # If the Nested key-value 
        # pair is of list type 
        elif type(x) is list: 
              
            i = 0
              
            for a in x:                 
                flatten(a, name + str(i) + '_') 
                i += 1
        else: 
            out[name[:-1]] = x 
  
    flatten(y) 
    return out 

def get_leaves(item, key=None):
    if isinstance(item, dict):
        leaves = {}
        for i in item.keys():
            leaves.update(get_leaves(item[i], i))
        return leaves
    elif isinstance(item, list):
        leaves = {}
        for i in item:
            leaves.update(get_leaves(i, key))
        return leaves
    else:
        return {key : item}


# with open('json.txt') as f_input:
#     json_data = json.load(f_input)
print("Begin code...")

urlcomp = ''
# API Call to retrieve report
print(" API Call to retrieve report..")
rcomp = requests.get(urlcomp)
## API Results
unflat_json = rcomp.json()
json_data=flatten_json(unflat_json)
# First parse all entries to get the complete fieldname list
fieldnames = set()
print(rcomp)
# for entry in json_data:
#     fieldnames.update(get_leaves(entry).keys())

# with open('output.csv', 'w', newline='') as f_output:
#     csv_output = csv.DictWriter(f_output, fieldnames=sorted(fieldnames))
#     csv_output.writeheader()
#     csv_output.writerows(get_leaves(entry) for entry in json_data)