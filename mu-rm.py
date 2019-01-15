#!/usr/bin/python

import sys
import json
import pymongo

from pymongo import MongoClient
from bson.objectid import ObjectId
from bson.json_util import dumps


mongo = MongoClient('localhost', 27017)
ingresslog = mongo.ingressmu.ingressmu

sys.argv.pop(0)
for id in sys.argv:
	#print id
	print
	query={"_id": ObjectId(id)}
	res= ingresslog.find(query,None)
	for rec in res:
		print dumps(rec)
	ingresslog.remove(query)

