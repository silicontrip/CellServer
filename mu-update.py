#!/usr/bin/python

import sys
import json
import pymongo

from pymongo import MongoClient
from bson.objectid import ObjectId


mongo = MongoClient('localhost', 27017)
ingresslog = mongo.ingressmu.ingressmu

id = sys.argv[1]
mu = sys.argv[2]
print id
print mu
ingresslog.update_one({"_id": ObjectId(id)},{"$set": { "mu": mu} }, upsert=False)

