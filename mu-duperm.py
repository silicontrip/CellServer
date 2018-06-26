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
	key= (rec['agent'],rec['ent'][0])
	if key in dups:
		id = rec['_id']
		ingresslog.remove({"_id": ObjectId(id)})
		dupcount +=1
	else:
		dups.add(key)	
		distcount +=1
	
print distcount,dupcount
