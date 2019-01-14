#!/usr/bin/python

import json
import pymongo
import sys
from pymongo import MongoClient
from bson.json_util import loads

mongo = MongoClient('localhost', 27017)

mdb = mongo.ingressmu
ingresslog = mdb.ingressmu

decode=loads(sys.argv[1])
print decode
ingresslog.insert(decode,check_keys=False)
