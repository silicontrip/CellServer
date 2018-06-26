#!/usr/bin/python

import sys
import json
import pymongo
import math

from pymongo import MongoClient
from bson.objectid import ObjectId
from bson.json_util import dumps

def angdistance (p1, p2):

        lat1= float(p1['latE6']) / 1000000.0
        lng1= float(p1['lngE6']) / 1000000.0
        lat2= float(p2['latE6']) / 1000000.0
        lng2= float(p2['lngE6']) / 1000000.0

        dLat = math.radians(lat2-lat1)
        dLng = math.radians(lng2-lng1)

        a = math.sin(dLat/2) * math.sin(dLat/2) + math.cos(math.radians(lat1)) * math.cos(math.radians(lat2)) * math.sin(dLng/2) * math.sin(dLng/2)
        return  2 * math.atan2(math.sqrt(a), math.sqrt(1-a));

def angarea (points):
	a=angdistance(points[0],points[1])
	b=angdistance(points[1],points[2])
	c=angdistance(points[2],points[0])

	s = (a+b+c)/2
	return 4 * math.atan(math.sqrt(math.tan(s/2) * math.tan((s-a)/2) * math.tan ((s-b)/2) * math.tan ((s-c)/2)))

mongo = MongoClient('localhost', 27017)
ingresslog = mongo.ingressmu.ingressmu

dt = json.loads(sys.argv[1])

for ff in dt:

	llE6=[]
	for pt in ff["latLngs"]:
	#	print pt
		llE6.append({ "latE6": int(pt["lat"] * 1000000), "lngE6": int(pt["lng"]*1000000)})

	#print llE6

	query = {"data.points": { "$all" :  [ {"$elemMatch" : llE6[0]}, {"$elemMatch" : llE6[1]}, {"$elemMatch" : llE6[2]}]}} 
	#print query
	res= ingresslog.find(query,None)
	for rec in res:
		ts = rec["timestamp"]

#	print ts
	tsl = ts - 5000
	tsu = ts + 5000
	query = { "$and" : [ {"$or" : [ {"data.points": { "$all" :  [ {"$elemMatch" : llE6[0]}, {"$elemMatch" : llE6[1]}]} }, {"data.points": { "$all" :  [ {"$elemMatch" : llE6[1]}, {"$elemMatch" : llE6[2]}]} }, {"data.points": { "$all" :  [ {"$elemMatch" : llE6[2]}, {"$elemMatch" : llE6[0]}]} } ]} , {"timestamp": { "$gt": tsl}}, {"timestamp": { "$lt": tsu}}  ]} 
#	print query
	res= ingresslog.find(query,None)
	oid=[]	
	mu=[]	
	if res.count()==2:
		for rec in res:
			print dumps(rec)
			oid.append(rec['_id'])
			mu.append(rec['mu'])
			pts = rec['data']['points']
			print angarea(pts) * 6367 * 6367
			print 

		print oid	
		print mu
		
		ingresslog.update_one({"_id": oid[0]},{"$set": { "mu": mu[1]} }, upsert=False)
		ingresslog.update_one({"_id": oid[1]},{"$set": { "mu": mu[0]} }, upsert=False)

