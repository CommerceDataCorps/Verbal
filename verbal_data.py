# Half of this can probably go
import csv, pprint, json, urllib2, random, time, datetime, pickle, requests, fiona
import numpy as np
from censusgeocode import CensusGeocode
from retry import retry
from shapely.geometry import Point, shape, MultiPoint, MultiPolygon, Polygon, mapping
from osgeo import ogr
from tqdm import *
from rtree import *

pp = pprint.PrettyPrinter(indent=2)

###########################################################################################################################
# Misc
# Must build spatial index before attempting Lat/Lon Conversion using build_index function
# See geocode_local.py for example
# Zip to Tract Conversion needs relationship file from US Census.
# See ziptract.py for example
###########################################################################################################################

class FastRtree(Rtree):
	def dumps(self, idx):
		return pickle.dumps(idx, -1)

def build_index(fc):
	for i, obj in tqdm(enumerate(fc)):
		tract = shape(obj['geometry'])
		yield (i, tract.bounds, obj)

def write_index(fc, p):
	idx = index.Index(p.filename, build_index(fc), properties=p, overwrite=True)

def loadsofar(fname):
	with open(fname) as data:
		jsonfile = json.load(data)
	return jsonfile

def savesofar(fname, dataset):
	with open(fname, 'w') as dump:
		json.dump(dataset, dump)

def coord_tract(lat, lon):
	url = "http://data.fcc.gov/api/block/find?format=json&latitude=%s&longitude=%s&showall=true"%(lat, lon)
	try:
		result = urllib2.urlopen(url)
		fips = json.loads(result.read())['Block']['FIPS']
		if fips is not None:
			return fips[0:11]
		else:
			return "Nothing"
	except urllib2.URLError, e:
  		print "Shit if I know?  ", e

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

@retry(delay=1, backoff=2, tries=10, jitter=(0,5))
def batch_geocode(fname):
	url = 'http://geocoding.geo.census.gov/geocoder/geographies/addressbatch'
	payload = {'benchmark':'Public_AR_Current','vintage':'Current_Current'}
	files = {'addressFile': (fname, open(fname, 'rb'), 'text/csv')}
	r = requests.post(url, files=files, data = payload)
	splitme = r.text.split('\n')
	return splitme

def batch_addr_geocode(dataset, addr_lst, name=''):
	unid = 0
	batch = []
	all_batches = []
	while unid < len(dataset):
		row = dataset[unid]
		batch.append([unid, row[addr_lst[0]], row[addr_lst[1]], row[addr_lst[2]], row[addr_lst[3]]])
		if (unid % 1000 == 999) or (unid == len(dataset) - 1):
			fname = '%s_batch_addr_csv'%name
			with open(fname, 'wb') as csvdump:
			    wr = csv.writer(csvdump)
			    for row in batch:
			    	wr.writerow(row)
			next_batch = batch_geocode(fname)
			pp.pprint(next_batch)
			# print len(next_batch)
			for row in next_batch:
				rowlist = row.split('\",\"')
				if len(rowlist) != 1:
					if len(rowlist) == 12:
						all_batches.append([int(rowlist[0][1:]),'%s%s%s'%(rowlist[8],rowlist[9],rowlist[10])])
					else:
						all_batches.append([int(rowlist[0][1:]),None])
			batch = []		
		unid += 1
	return all_batches

@retry((ValueError, TypeError), delay=1, backoff=2, tries=10, jitter=(0,1))
def geocodeme(row, addr_lst):
	cg = CensusGeocode()
	result = cg.address(row[addr_lst[0]], city=row[addr_lst[1]], state=row[addr_lst[2]], zipcode=row[addr_lst[3]])
	if len(result) != 0:
		return result[0]['geographies']['Census Tracts'][0]['GEOID']
	else:
		return None

def geocode_local(coordinates):
	size = len(coordinates)
	print 'Geocoding: ' + '%09d'%size + 'pts'
	geo_tract = range(0,size)
	retain_index = [ True for i in range(0,size) ]
	with fiona.open('./tracts/us_all_tracts.shp') as us_tracts:
		for i in tqdm(random.sample(range(0,len(us_tracts)), len(us_tracts))):
			feature = us_tracts[i]
			p = prep(shape(feature['geometry']))
			geoid = feature['properties']['GEOID10']
			# print 'Processing: ' + geoid
			search_coord = MultiPoint([ coordinates[i] for i in range(0,size) if retain_index[i] == True ])
			true_index = [ i for i in range(0,size) if retain_index[i] == True ]
			index = map(p.contains, search_coord)
			for i in range(0,len(index)):
				if index[i] == True:
					geo_tract[true_index[i]] = geoid
					retain_index[true_index[i]] = False
			if retain_index.count(True) == 0:
				return geo_tract
	return geo_tract

###########################################################################################################################
# Test Coordinate
###########################################################################################################################
# print coord_tract(51.518144, 30.766847)

# cg = CensusGeocode()

# result = cg.coordinates(x=-76, y=41)
# result = cg.onelineaddress('1600 Pennsylvania Avenue, Washington, DC')
# result = cg.address('1600 Pennsylvania Avenue', city='Washington', state='DC', zipcode='22052')

# pp.pprint(result.input)
# pp.pprint(result)

# print '*****************************************************************************\n\n\n\n'

# result = cg.onelineaddress('2700 Pennsylvania Avenue, Washington, DC')

# pp.pprint(result.input)
# pp.pprint(result)

# row = ['857 State Hwy 35', 'Middletown', 'New Jersey', '07748']
# tract = geocodeme(row, (0,1,2,3))
# print tract

###########################################################################################################################
# Build list of all 2010 Census tracts
# This defines the initial verbal dictionary keyed at the US Tract level
# Available @ https://www.census.gov/geo/maps-data/data/tract_rel_download.html
###########################################################################################################################
verbal_list = {}
with open("us2010trf.txt") as data:
	csvfile = csv.reader(data)
	duplicates = []
	for row in csvfile:
		tract = row[9]+row[10]+row[11]
		duplicates.append(tract)

unduplicate = set(duplicates)

for trct in unduplicate:
	verbal_list[trct] = {}

###########################################################################################################################
# Load data if building dataset piecemeal
###########################################################################################################################
verbal_list = loadsofar("verbal_list.json")

###########################################################################################################################
# Census Employment by Industry
# Condense to three digit NAICS Code and append to Verbal dictionary at Tract level
# Available @ ftp://ftp.census.gov/econ2013/CBP_CSV/cbp13co.zip
###########################################################################################################################
naics_list = {}
three_digit_naics = []
with open("cbp13co.txt") as data:
	csvfile = csv.reader(data)
	next(csvfile)
	duplicates = []
	print 'Loading NAICS Employment information:'
	for row	in tqdm(csvfile):
		if row[4] != 'D' and row[4] != 'S' and row[2][3:6] == '///':
			county = row[0]+row[1]
			three_digit_naics.append(row[2])
			if naics_list.has_key(county):
				naics_list[county][row[2]] = row[5]
			else:
				naics_list[county] = {row[2]:row[5]}

three_digit_naics = list(set(three_digit_naics))
print 'Complete'

print 'Appending to Verbal list:'
for trct in tqdm(verbal_list.keys()):
	verbal_list[trct]['employment'] = {}
	for naics in three_digit_naics:
		verbal_list[trct]['employment'][naics[0:3]] = 0
	if naics_list.has_key(trct[0:5]):
		for naics in naics_list[trct[0:5]].keys():
			verbal_list[trct]["employment"][naics[0:3]] = naics_list[trct[0:5]][naics]
print 'Complete'

for trct in verbal_list.keys()[:50]:
	pp.pprint(len(verbal_list[trct]['employment'].keys()))
	pp.pprint(verbal_list[trct]['employment'])

###########################################################################################################################
# NPR Playground Data
# Available @ http://www.playgroundsforeveryone.com/
###########################################################################################################################
print 'Loading MPR Playground Data:'
with open("npr-accessible-playgrounds.json") as data:
	npr_ind = json.load(data)
playground_coord = []
for pg in tqdm(npr_ind['playgrounds']):
	playground_coord.append([float(pg['latitude']), float(pg['longitude'])])
print 'Complete'

print 'Load Spatial Index:'
fc = fiona.open('./tracts/us_all_tracts.shp')
p = index.Property()
p.overwrite = True
p.filename = 'us_tracts_index'
idx = Rtree(p.filename, properties=p)
print 'Complete'

print 'Gecoding %09d points:'%len(playground_coord)
for i, pt in tqdm(enumerate(playground_coord)):
	coord = (pt[1],pt[0])
	tract =  list(idx.nearest(coord))[0]
	playground_coord[i].append(fc[tract]['properties']['GEOID10'])
print 'Complete'

print 'Processing for append:'
playground_append = {}
for pg in tqdm(playground_coord):
	tract = pg[2]
	if playground_append.has_key(tract) == False:
		playground_append[tract] = 0
	playground_append[tract] += 1
print 'Complete'

print 'Appending NPR Playground counts:'
for trct in verbal_list.keys():
	verbal_list[trct]['playgrounds'] = 0
	if playground_append.has_key(trct):
		verbal_list[trct]['playgrounds'] = playground_append[trct]
print 'Complete'

for tract in verbal_list.keys()[0:10000]:
	pp.pprint(verbal_list[tract]['playgrounds'])

###########################################################################################################################
# FDIC Locations
# Available @ https://www5.fdic.gov/idasp/warp_download_all.asp
###########################################################################################################################
fdic_loc = loadinput('./fdic_Offices2/OFFICES2_ALL.CSV','csv')

fdic_list = batch_addr_geocode(fdic_loc, (0,12,27,29), name='fdic_loc')

fdic_cnt = {x:fdic_list.count(x) for x in fdic_list}

for trct in fdic_cnt.keys():
	if trct is not None:
		verbal_list[trct]['fdic'] = {'loc_cnt': fdic_cnt[trct]}
		pp.pprint(verbal_list[trct])

print 'Append FDIC Zeroes:'
for tract in tqdm(verbal_list.keys()):
	if verbal_list[tract].has_key('fdic'):
		if verbal_list[tract]['fdic'].has_key('loc_cnt') == False:
			verbal_list[tract]['fdic']['loc_cnt'] = 0
		if verbal_list[tract]['fdic'].has_key('assets') == False:
			verbal_list[tract]['fdic']['assets'] = 0
	else:
		verbal_list[tract]['fdic'] = {'loc_cnt':0, 'assets':0}
print 'Complete'

for tract in verbal_list.keys():
	pp.pprint(verbal_list[tract]['fdic'])

###########################################################################################################################
# FDIC Holdings
# Available @ http://www.consumerfinance.gov/complaintdatabase/
###########################################################################################################################
fdic_hold = loadinput('./fdic_Institutions2/INSTITUTIONS2.CSV', 'csv')
fdic_holdings = []
for row in fdic_hold[1:]:
	if len(row[70]) == 4:
		fixzip = '0%s'%row[70]
	else:
		fixzip = row[70]
	fdic_holdings.append([row[4], row[15], row[0], fixzip, row[5]])

fdic_list = batch_addr_geocode(fdic_holdings, (0,1,2,3), name='fdic_holdings')

fdic_sum = {}
for row in fdic_list:
	row_id = row[0]
	tract = row[1]
	if fdic_holdings[row_id][4] == '':
		assets = 0
	else:
		assets = int(fdic_holdings[row_id][4].replace(',',''))
	if tract != None:
		if tract not in fdic_sum.keys():
			fdic_sum[tract] = assets
		else:
			fdic_sum[tract] += assets

pp.pprint(fdic_sum)

for trct in fdic_sum.keys():
	if trct is not None:
		if 'fdic' in verbal_list[trct].keys():
			verbal_list[trct]['fdic']['assets'] = fdic_sum[trct]
		else:
			verbal_list[trct]['fdic'] = {'assets': fdic_sum[trct]}

###########################################################################################################################
# Public Libraries
# Not complete listing of all locations, only a survey
# Available @ https://www.imls.gov/research-evaluation/data-collection/public-libraries-united-states-survey
###########################################################################################################################
libraries = loadinput('./pupld13a_csv/Puout13a.csv','csv')

for library in libraries[1:3]:
	print library[27]
	print library[28]
	print '%06d'%int(library[32].replace('.',''))
	tract = '%s%s%s'%(library[27],library[28],'%06d'%int(library[32].replace('.','')))
	print tract
###########################################################################################################################
# Yelp Datset
# Lat/Lon Conversion
# Not Complete Listing, only academic challenge dataset
# Available @ http://www.yelp.com/dataset_challenge
###########################################################################################################################
print 'Loading Yelp Academic Dataset:'
all_businesses = []
categories = {}
with open('./yelp_dataset_challenge_academic_dataset/yelp_academic_dataset_business.json') as yelp:
	for line in tqdm(yelp):
		business = json.loads(line)
		all_businesses.append([business['latitude'],business['longitude'],business['categories']])
		for cat in business['categories']:
			if cat not in categories.keys():
				categories[cat] = 0
print 'Complete'

print 'Load Spatial Index:'
fc = fiona.open('./tracts/us_all_tracts.shp')
p = index.Property()
p.overwrite = True
p.filename = 'us_tracts_index'
idx = Rtree(p.filename, properties=p)
print 'Complete'

print 'Gecoding %09d points:'%len(all_businesses)
for i, bus in tqdm(enumerate(all_businesses)):
	coord = (bus[1],bus[0])
	tract =  list(idx.nearest(coord))[0]
	all_businesses[i].append(fc[tract]['properties']['GEOID10'])
print 'Complete'

print 'Processing for append:'
yelp_append = {}
for bus in tqdm(all_businesses):
	tract = bus[3]
	bus_cat = bus[2]
	if tract not in yelp_append.keys():			
		yelp_append[tract] = {}
		for key in categories.keys():			
			yelp_append[tract][key] = 0
	for cat in bus_cat:
		yelp_append[tract][cat] += 1
print 'Complete'

# pp.pprint(yelp_append)
print 'Appending Yelp:'
for trct in tqdm(verbal_list.keys()):
	if yelp_append.has_key(trct):
		verbal_list[trct]['yelp'] = yelp_append[trct]
	else:
		verbal_list[trct]['yelp'] = {}
		for cat in categories.keys():
			verbal_list[trct]['yelp'][cat] = 0
print 'Complete'
###########################################################################################################################
# Education
# Lat/Lon Conversion
# Generated for Teacher to Pupil Ratio by Level, # of Teachers (FTEs) by Level, and School Size by Level
# Available @ https://nces.ed.gov/ccd/elsi/tableGenerator.aspx
###########################################################################################################################
schools = loadinput('./education_elsi.csv','csv')

print 'Loading ELSI dataset:'
elsi = []
coordinates = []
for school in tqdm(schools[1:]):
	if school[7] != 'n/a':
		elsi.append([float(school[8]),float(school[7]),school[2],school[3],school[9]])
		coordinates.append((float(school[8]),float(school[7])))
print 'Complete'

print 'Loading Spatial Index:'
fc = fiona.open('./tracts/us_all_tracts.shp')
p = index.Property()
p.overwrite = True
p.filename = 'us_tracts_index'
idx = Rtree(p.filename, properties=p)
print 'Complete'

print 'Gecoding %09d points:'%len(elsi)
for i, school in tqdm(enumerate(elsi)):
	coord = (school[0],school[1])
	tract =  list(idx.nearest(coord))[0]
	elsi[i].append(fc[tract]['properties']['GEOID10'])
print 'Complete'

print 'Processing for append:'
elsi_final = {}
for school in tqdm(elsi):
	if school[5] in elsi_final.keys():
		elsi_final[school[5]]['pup_teach_ratio'].append(school[2])
		elsi_final[school[5]]['teach_total'].append(school[3])
		elsi_final[school[5]]['pupil_total'].append(school[4])
	else:
		elsi_final[school[5]] = {'pup_teach_ratio':[school[2]], 'teach_total':[school[3]], 'pupil_total':[school[4]]}

elsi_append = {}
for trct in tqdm(elsi_final.keys()):
	elsi_append[trct] = {}
	for cat in elsi_final[trct].keys():
		avgme = [ float(x) for x in elsi_final[trct][cat] if x != b'\xe2\x80\xa0' and x != b'\xe2\x80\x93' ]
		if len(avgme) != 0:
			elsi_append[trct][cat] = sum(avgme)/len(avgme) 

print 'Complete'

# pp.pprint(elsi_append)
print 'Appending ELSI:'
for trct in tqdm(verbal_list.keys()):
	verbal_list[trct]['education'] = {'pup_teach_ratio':0, 'teach_total':0, 'pupil_total':0}
	if trct in elsi_append.keys():
		verbal_list[trct]['education'] = elsi_append[trct]
print 'Complete'
###########################################################################################################################
# Financial Complaints
# Zip Conversion
# Available @ http://www.consumerfinance.gov/complaintdatabase/
###########################################################################################################################
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

# pp.pprint(compl_append)

print 'Appending to Verbal:'
for tract in tqdm(verbal_list.keys()):
	if compl_append.has_key(tract):
		verbal_list[tract]['financial complaints'] = compl_append[tract][0]/compl_append[tract][1]
	else:
		verbal_list[tract]['financial complaints'] = 0
print 'Complete'

for tract in verbal_list.keys():
	if len(verbal_list[tract].keys()) == 9:
		print len(verbal_list[tract].keys())
		pp.pprint(verbal_list[tract].keys())

###########################################################################################################################
# Public resources
# Lat/Lon Conversion
# Available @ http://usda.github.io/RIDB/
# Just pulled the underlying database
###########################################################################################################################
print 'Loadig Public Resources:'
facility = loadinput('./RIDBFullExport_v1/Facilities_API_v1.csv','csv')
recreation = loadinput('./RIDBFullExport_v1/RecAreas_API_v1.csv','csv')
print 'Loading Facilities:'
pub_fac = []
for fac in tqdm(facility[1:]):
	if fac[5] != '' and fac[6] != '':
			pub_fac.append([float(fac[5]), float(fac[6]), u'facility'])
print 'Complete'
print 'Loading Recreation Areas:'
pub_rec = []
for rec in tqdm(recreation[1:]):
	if rec[8] != '' and rec[9] != '':
		pub_rec.append([float(rec[8]), float(rec[9]), u'recreation'])
print 'Complete'
public = pub_fac + pub_rec
print 'Load Complete'

print 'Load Spatial Index:'
fc = fiona.open('./tracts/us_all_tracts.shp')
p = index.Property()
p.overwrite = True
p.filename = 'us_tracts_index'
idx = Rtree(p.filename, properties=p)
print 'Complete'

print 'Gecoding %09d points:'%len(public)
for i, bus in tqdm(enumerate(public)):
	coord = (bus[1],bus[0])
	tract =  list(idx.nearest(coord))[0]
	public[i].append(fc[tract]['properties']['GEOID10'])
print 'Complete'

# pp.pprint(public)

print 'Processing for append:'
public_append = {}
for place in tqdm(public):
	tract = place[3]
	place_cat = place[2]
	if tract not in public_append.keys():			
		public_append[tract] = {u'public':0, u'facility':0, u'recreation':0}
	public_append[tract][place_cat] += 1
	public_append[tract]['public'] += 1
print 'Complete'

# pp.pprint(public_append)
print 'Appending Public Resources:'
for trct in verbal_list.keys():
	if trct in public_append.keys():
		verbal_list[trct]['public resources'] = public_append[trct]
print 'Complete'

print 'Adding Public Resources Zero:'
for tract in tqdm(verbal_list.keys()):
	if verbal_list[tract].has_key('public resources') == False:
		verbal_list[tract]['public resources'] = {u'public':0, u'facility':0, u'recreation':0}
print 'Complete'

for tract in verbal_list.keys():
	pp.pprint(verbal_list[tract]['public resources'])

###########################################################################################################################
# Museum Universe
# Available @ https://www.imls.gov/research-evaluation/data-collection/museum-universe-data-file
###########################################################################################################################
museums = loadinput('./museum_mudf15q1pub.csv','csv')

mus_uni = []
for museum in museums[1:]:
	if museum[26] != '':
		mus_uni.append('%02d%03d%06d'%(int(museum[26]),int(museum[27]),int(museum[28])))

mus_uni_cnt = {x:mus_uni.count(x) for x in mus_uni}

# pp.pprint(mus_uni_cnt)
for trct in mus_uni_cnt.keys():
	verbal_list[trct]['museum'] = mus_uni_cnt[trct]

print 'Add museum zeros:'
for tract in tqdm(verbal_list.keys()):
	if verbal_list[tract].has_key('museum') == False:
		verbal_list[tract]['museum'] = 0
print 'Complete'

for tract in verbal_list.keys():		
	print len(verbal_list[tract].keys())
	pp.pprint(verbal_list[tract]['museum'])

###########################################################################################################################
# Zillows Data
# Zip Conversion
# Available @ http://www.zillow.com/research/data/
###########################################################################################################################
median_price = loadinput('./Zip_MedianSoldPrice_AllHomes.csv', 'csv')
median_price_sqft = loadinput('./Zip_MedianSoldPricePerSqft_AllHomes.csv', 'csv')
num_rent = loadinput('./Zip_NumberOfHomesForRent_AllHomes.csv', 'csv')
price_rent_ratio = loadinput('./Zip_PriceToRentRatio_AllHomes.csv', 'csv')
turnover = loadinput('./Zip_Turnover_AllHomes.csv', 'csv')

def add_zillows(dataset, varname, final):
	for listing in tqdm(dataset[1:]):
		zipcode = listing[0]
		if final.has_key(zipcode):
			final[zipcode][varname] = listing[-1]
		else:
			final[zipcode] = {varname:listing[-1]}
	return final

print 'Loading Zillows Dataset:'
zillows = {}
add_zillows(median_price, 'median_price', zillows)
add_zillows(median_price_sqft, 'median_price_sqft', zillows)
add_zillows(num_rent, 'num_rent', zillows)
add_zillows(price_rent_ratio, 'price_rent_ratio', zillows)
add_zillows(turnover, 'turnover', zillows)

categories = ['median_price', 'median_price_sqft', 'num_rent', 'price_rent_ratio', 'turnover']

for zipcode in tqdm(zillows.keys()):
	for cat in categories:
		if zillows[zipcode].has_key(cat) == False:
			zillows[zipcode][cat] = None
		elif zillows[zipcode][cat] == '':
			zillows[zipcode][cat] = None
print 'Complete'

print 'Migrating to Census Tract:'
ziptract = [ [(x[0],x[4]),float(x[18])/100] for x in loadinput('./zcta_tract_rel_10.txt', 'csv')[1:] ]
for i, row in tqdm(enumerate(ziptract)):
	zipcode = row[0][0]
	if zillows.has_key(zipcode):
		for cat in zillows[zipcode].keys():
			try:
				ziptract[i].append(float(zillows[zipcode][cat]))
			except (TypeError, ValueError):
				ziptract[i].append(None)
	else:
		ziptract[i].extend([None,None,None,None,None])
print 'Complete'

print 'Counting Census Tract:'
zillows_append = {}
for row in tqdm(ziptract):
	tract = row[0][1]
	if zillows_append.has_key(tract) == False:
		zillows_append[tract] = range(2,7)
		for i in range(2,7):
			if row[i] != None:
				zillows_append[tract][i-2] = [float(row[i]),1]
			else:
				zillows_append[tract][i-2] = [0.0,0]
	else:
		for i in range(2,7):
			if row[i] != None:
				print zillows_append[tract][i-2]
				zillows_append[tract][i-2][0] += float(row[i])
				zillows_append[tract][i-2][1] += 1
print 'Complete'

print 'Appending to Verbal:'
for tract in tqdm(verbal_list.keys()):
	if zillows_append.has_key(tract):
		add_dict = {}
		for i, cat in enumerate(categories):
			if zillows_append[tract][i][1] != 0:
				avg = zillows_append[tract][i][0]/zillows_append[tract][i][1]
			else:
				avg = None
			add_dict[cat] = avg
		verbal_list[tract]['zillows'] = add_dict
	else:
		verbal_list[tract]['zillows'] = {}
		for cat in categories:
			verbal_list[tract]['zillows'][cat] = None
print 'Complete'

###########################################################################################################################
# Broadband
# Available @ http://www.broadbandmap.gov/data.json
###########################################################################################################################
with open('./All-NBM-CSV-June-2014/NATIONAL-NBM-Address-Street-CSV-JUN-2014.CSV') as loadfile:
	broadband = {}
	loadfile_map = csv.reader(loadfile, delimiter='|')
	loadfile_map.next()
	for row in loadfile_map:
		if row[9][0:11] in broadband.keys():
			broadband[row[9][0:11]]['down'].append(int(row[16]))
			broadband[row[9][0:11]]['up'].append(int(row[17]))	
		else:
			broadband[row[9][0:11]] = {'up':[int(row[16])], 'down':[int(row[17])]}

broadband_speeds = {}
for trct in broadband.keys():
	upspeed = broadband[trct]['up']
	downspeed = broadband[trct]['down']
	broadband_speeds[trct] = {}
	broadband_speeds[trct]['up'] = sum(upspeed)/float(len(upspeed))
	broadband_speeds[trct]['down'] = sum(downspeed)/float(len(downspeed))

for trct in broadband_speeds.keys():
	verbal_list[trct]['broadband'] = broadband_speeds[trct]

print 'Add Broadband zeroes:'
for tract in tqdm(verbal_list.keys()):
	if verbal_list[tract].has_key('broadband') == False:
		verbal_list[tract]['broadband'] = {'up':0, 'down':0}
print 'Complete'

min = 10
for tract in verbal_list.keys():
	if len(verbal_list[tract].keys()) < min:
		print len(verbal_list[tract].keys())
		min = len(verbal_list[tract].keys())
	pp.pprint(verbal_list[tract].keys())

###########################################################################################################################
# USDA
# Available @ http://www.ers.usda.gov/data-products/food-environment-atlas/data-access-and-documentation-downloads.aspx
###########################################################################################################################
# Access [4] ['PCT_LACCESS_POP10']
access = loadinput('./usda/ACCESS-Table 1.csv', 'csv')
# Stores [4,10,16,22,28,34] ['GROC12', 'SUPERC12', 'CONVS12', 'SPECS12', 'SNAPS12', 'WICS12']
stores = loadinput('./usda/STORES-Table 1.csv', 'csv')
# Restaurants [4,10,16,18] ['FFR12', 'FSR12', 'PC_FFRSALES07', 'PC_FSRSALES07']
rest = loadinput('./usda/RESTAURANTS-Table 1.csv', 'csv')
# SNAP [7,10] ['PCT_SNAP14', 'PC_SNAPBEN10']
snap = loadinput('./usda/ASSISTANCE-Table 1.csv','csv')
# Farmers Markets [9] ['FMRKT13']
farmer = loadinput('./usda/LOCAL-Table 1.csv', 'csv')

usda = {}
def add_usda(dataset, pullvar, varname):
	for row in dataset[1:]:
		if row[0] in usda.keys():
			for i in range(0,len(pullvar)):
				usda[row[0]][varname[i]] = row[pullvar[i]]
		else:
			usda[row[0]] = {}
			for i in range(0,len(pullvar)):			
				usda[row[0]][varname[i]] = row[pullvar[i]]

add_usda(access, [4], ['PCT_LACCESS_POP10'])
add_usda(stores, [4,10,16,22,28,34], ['GROC12', 'SUPERC12', 'CONVS12', 'SPECS12', 'SNAPS12', 'WICS12'])
add_usda(rest, [4,10,16,18], ['FFR12', 'FSR12', 'PC_FFRSALES07', 'PC_FSRSALES07'])
add_usda(snap, [7,10], ['PCT_SNAP14', 'PC_SNAPBEN10'])
add_usda(farmer, [9], ['FMRKT13'])

for trct in verbal_list.keys():
	if usda.has_key(trct[0:5]):
		verbal_list[trct]['usda'] = usda[trct[0:5]]

###########################################################################################################################
# Dump to file
# If building piecemeal, save everything loaded so far so you don't repeat it
###########################################################################################################################
savesofar("verbal_list.json", verbal_list)

###########################################################################################################################
# Convert to CSV
# If you want flat CSV instead
###########################################################################################################################
schema = ['tract']
search = []
body = []

for tract in tqdm(verbal_list.keys()):
	row = [tract]
	for dataset in verbal_list[tract].keys():
		if isinstance(verbal_list[tract][dataset], dict):
			for var in verbal_list[tract][dataset].keys():
				if len(schema) < 901:
					schema.append('%s_%s'%(dataset[0:4],var))
					search.append((dataset,var))
		else:
			if len(schema) < 901:
				schema.append(dataset[0:4])
				search.append((dataset,))

for tract in tqdm(verbal_list.keys()):
	row = [tract]
	for entry in search:
		dataset = entry[0]
		if len(entry) == 2:
			var = entry[1]
			if verbal_list[tract][dataset].has_key(var):
				if verbal_list[tract][dataset][var] != None and verbal_list[tract][dataset][var] != '':
					row.append(float(verbal_list[tract][dataset][var]))
				else:
					row.append(None)
			else:
				row.append(None)
		else:
			if verbal_list[tract][dataset] != None:
				row.append(float(verbal_list[tract][dataset]))
			else:
				row.append(None)
			if len(schema) < 901:
				schema.append(dataset[0:4])
	body.append(row)

final = [schema]
final.extend(body)
rowlength = len(final[0])
print rowlength
for row in final:
  if len(row) != rowlength:
    print len(row)

with open('./verbal_list.csv', 'wb') as csvfile:
	verbal_csv = csv.writer(csvfile, delimiter=',')
	verbal_csv.writerows(final)
# exp_csv = np.asarray(final)
# np.savetxt('./verbal_list.csv', exp_csv, delimiter=',', fmt='%s')