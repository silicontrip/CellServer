#!/usr/bin/python

import json
import pymongo
import sys
from pymongo import MongoClient
from bson.objectid import ObjectId

from bson.json_util import loads

mongo = MongoClient('localhost', 27017)

mdb = mongo.ingressmu
ingresslog = mdb.mu

ingresslog.drop()

ign = open (sys.argv[1],'r')
cellmu = json.loads(ign.read())


for cell in cellmu:
	celldata = cellmu[cell]
	#print celldata
	query = {"cell": cell}
	muobj = {"cell": cell, "mu": celldata, "_id": cell}

	ingresslog.replace_one(query,muobj,upsert=True)

