#!/usr/bin/python

import json
import pymongo
from pymongo import MongoClient
from bson.json_util import dumps

mongo = MongoClient('localhost', 27017)

mdb = mongo.ingressmu
ingresslog = mdb.mu

res= ingresslog.find({},None)


for rec in res:
	print dumps(rec)
