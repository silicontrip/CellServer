#!/usr/bin/python

import json
import pymongo
import sys
from pymongo import MongoClient
from bson.objectid import ObjectId
from bson.json_util import dumps

from bson.json_util import loads

mongo = MongoClient('localhost', 27017)

mdb = mongo.ingressmu
ingresslog = mdb.mu

cell=sys.argv[1]
muobj = {"cell": cell}

res=ingresslog.find(muobj,None)

for rec in res:
        print dumps(rec)

