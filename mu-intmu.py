#!/usr/bin/python

import sys
import json
import pymongo

from pymongo import MongoClient
from bson.objectid import ObjectId


mongo = MongoClient('localhost', 27017)
ingresslog = mongo.ingressmu.ingressmu

#print ingresslog.getIndexes()

#       print "creating index..."
#       ingresslog.create_index("path")
#       ingresslog.create_index("timing.client_ssl")
#       ingresslog.create_index("2.plext.markup")
#       print "index done."

res= ingresslog.find({},None)
dupcount=0
distcount=0
dups = set()
for rec in res:
	mut = rec["mu"]
	id = rec["_id"]
	distcount+=1
	#print type(mut)
	if (type(mut) == type(int())):
		ingresslog.update_one({"_id": id},{"$set": { "mu": str(mut)} }, upsert=False)
		dupcount+=1

	
print distcount,dupcount
