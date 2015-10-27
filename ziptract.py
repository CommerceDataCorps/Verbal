import csv, pprint, json, urllib2, random, time, datetime, pickle, requests, fiona, cPickle as pickle
import numpy as np
from retry import retry
from tqdm import *

pp = pprint.PrettyPrinter(indent=2)

def loadinput(fname, tp):
  	with open(fname) as data:
  		if tp == 'csv':
  			loadme = []
  			csvloadme = csv.reader(data)
  			for row in csvloadme:
  				loadme.append(row)
  		elif tp == 'json':
  			loadme = json.load(data)
  		elif tp == 'jsons':
  			loadme = json.loads(data)
  		else:
  			print 'Need defined filetype'
  	return loadme

# pp.pprint(ziptract[-1])
# for row in ziptract:
# 	if len(row) != 2:
# 		print len(row)
# 		print row

print 'Loading Complaints:'
complaints = loadinput('./Consumer_Complaints.csv','csv')
compl_list = []
for complaint in tqdm(complaints[1:]):
	compl_list.append(complaint[9])
print 'Complete'

print 'Counting Complaints:'
compl_cnt = {}
# This Craps out for some reason
# compl_cnt = {x:compl_list.count(x) for x in compl_list}
for zips in tqdm(compl_list):
	if 'X' not in zips:
		if compl_cnt.has_key(zips):
			compl_cnt[zips] += 1
		else:
			compl_cnt[zips] = 1
print 'Complete'

# pp.pprint(compl_cnt)

# pp.pprint(ziptract[-1])
# for row in ziptract:
# 	if len(row) != 2:
# 		print len(row)
# 		print row

print 'Migrating to Census Tract:'
ziptract = [ [(x[0],x[4]),float(x[18])/100] for x in loadinput('./zcta_tract_rel_10.txt', 'csv')[1:] ]
for i, row in tqdm(enumerate(ziptract)):
	zipcode = row[0][0]
	if compl_cnt.has_key(zipcode):
		ziptract[i].append(row[1]*compl_cnt[zipcode])
	else:
		ziptract[i].append(0)
print 'Complete'

print 'Counting Census Tract:'
compl_append = {}
for row in tqdm(ziptract):
	tract = row[0][1]
	if compl_append.has_key(tract) == False:
		compl_append[tract] = [row[2], 1]
	else:
		compl_append[tract][0] += row[2]
		compl_append[tract][1] += 1
print 'Complete'

pp.pprint(compl_append)