import csv, pprint, json, urllib2, random, time, datetime, pickle, requests, fiona, cPickle as pickle
import numpy as np
from retry import retry
from shapely.geometry import Point, shape, MultiPoint, MultiPolygon, Polygon, mapping
from shapely.prepared import prep
from multiprocessing import Process, Pool
from osgeo import ogr
from tqdm import *
from rtree import *

pp = pprint.PrettyPrinter(indent=2)

class FastRtree(Rtree):
	def dumps(self, idx):
		return pickle.dumps(idx, -1)

def build_index(fc):
	for i, obj in tqdm(enumerate(fc)):
		tract = shape(obj['geometry'])
		yield (i, tract.bounds, obj)

def loadinput(fname, tp):
  	with open(fname) as data:
  		if tp == 'csv':
  			loadme = []
  			csvloadme = csv.reader(data)
  			for row in tqdm(csvloadme):
  				loadme.append(row)
  		elif tp == 'json':
  			loadme = json.load(data)
  		elif tp == 'jsons':
  			loadme = json.loads(data)
  		else:
  			print 'Need defined filetype'
  	return loadme

print 'Building Index'
# idx = index.Index()
# with fiona.open('./tracts/us_all_tracts.shp') as us_tracts:
# 	for i, tract in tqdm(enumerate(us_tracts)):
# 		idx.insert(i, shape(tract['geometry']).bounds, tract)
p = index.Property()
p.overwrite = True
p.filename = 'us_tracts_index'
fc = fiona.open('./tracts/us_all_tracts.shp')
# idx = index.Index(p.filename, build_index(fc), properties=p, overwrite=True)
# file_idx = FastRtree()
idx = Rtree(p.filename, properties=p)
print 'Complete'


print 'Loading Coordinates'
schools = loadinput('./education_elsi.csv','csv')
elsi = []
coordinates = []
for school in tqdm(schools[1:]):
	if school[7] != 'n/a':
		# elsi.append([school[7],school[8],school[2],school[3],school[9]])
		coordinates.append((float(school[8]),float(school[7])))
print 'Complete'

pp.pprint(idx)

geoid = []
# coord = MultiPoint(coordinates)
for coord in tqdm(coordinates):
	# print list(idx.intersection(coord))
	tract =  list(idx.nearest(coord))[0]
	geoid.append(fc[tract]['properties']['GEOID10'])
