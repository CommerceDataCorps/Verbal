import csv, pprint, json, urllib2, msgpack, io
import numpy as np
import pandas as pd
import cPickle as pickle
from cStringIO import StringIO
from sklearn import preprocessing, decomposition
from tqdm import *
import matplotlib.pyplot as plt
from matplotlib.patches import Rectangle
from matplotlib.ticker import NullLocator

pp = pprint.PrettyPrinter(indent=2)

# load built dataset
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

# format into usable array
def build_X(backup=True, export=False):
    # Build X
    print 'Load inputs'
    X = []
    other_list = loadinput('./verbal_list.csv', 'csv')
    acs_list = loadinput('./data_acs5yr.csv', 'csv')
    acs_append = {}
    for gid in tqdm(acs_list[1:]):
    	# for whatever reason NHGIS zeropads its GID
    	tract = '%s%s%s'%(gid[0][1:3],gid[0][4:7],gid[0][8:])
    	acs_append[tract] = [ float(x) for x in gid[1:] ]
    for row in tqdm(other_list[1:]):
    	tract = [ row[0] ]
    	body = [ float(x) if x != '' else np.nan for x in row[1:] ]
    	tract.extend(body)
    	X.append(tract)
    print 'Complete'
    print 'Merging sets'
    for i, tract in tqdm(enumerate(X)):
    	empty = np.repeat(np.array([[np.nan]]),len(acs_append[acs_append.keys()[0]]))
    	if acs_append.has_key(tract[0]):
            X[i].extend(acs_append[tract[0]])
    	else:
            X[i].extend(empty)
    print 'Complete'

    schema = other_list[0] + acs_list[0][1:]
    datatype = [ (x, 'float') for x in schema[1:] ]
    if export == True:
        # Export to csv
        print 'Export flat file'
        X_out = [schema] + X
        with open('./verbal_final.csv', 'wb') as csvfile:
        	verbal_csv = csv.writer(csvfile, delimiter=',')
        	verbal_csv.writerows(X_out)
        print 'Complete'
    if backup == True:
        # Pickle X
        print 'Backup to pickle cache'
        with open('./verbal_cache.p', 'wb') as f:
            pickle.dump(X, f, protocol=-1)
            pickle.dump(schema, f, protocol=-1)

# Incomplete Hinton Graph to show PCA component weights
def hinton(W, ax=None):
    """
    Draws a Hinton diagram for visualizing a weight matrix.
    """
    if not ax:
        fig = plt.figure()
        ax = fig.add_subplot(1, 1, 1)

    # if not maxWeight:
    #     maxWeight = 2**np.ceil(np.log(np.abs(W).max())/np.log(2))

    ax.patch.set_facecolor('gray')
    ax.set_aspect('equal', 'box')
    ax.xaxis.set_major_locator(NullLocator())
    ax.yaxis.set_major_locator(NullLocator())

    for (x,y),w in np.ndenumerate(W):
        if w > 0: color = 'white'
        else:     color = 'black'
        size = np.sqrt(np.abs(w))
        rect = Rectangle([x - size / 2, y - size / 2], size, size,
            facecolor=color, edgecolor=color)
        ax.add_patch(rect)
    ax.autoscale_view()

    # Reverse the yaxis limits
    ax.set_ylim(*ax.get_ylim()[::-1])

# Run PCA, commented alternative decomposition methods
def pca_index(X, missing_values='impute', output=False):
    # X_array = np.asarray(np.delete(X,0,1),dtype=float)
    X_array = X.values
    comp_names = list(X.columns)
    if missing_values == 'impute':
    	# ACS data contains Inf values
    	X_array[np.isinf(X_array)] = np.nan
    	print 'Do basic imputation to take care of non finite values'
    	imp = preprocessing.Imputer(missing_values='NaN', strategy='mean', axis=0)
    	X_finite = imp.fit_transform(X_array)
    if missing_values == 'reduce':
        # Toss out the non finite observations
        print 'Tossing out all non finite observations'
        X_nomiss = X_array[~np.isnan(X_array).any(axis=1)]
        X_finite = X_nomiss[~np.isinf(X_nomiss).any(axis=1)]
    print 'Normalize features'
    X_normalized = preprocessing.normalize(X_finite, norm='l2')
    print 'PCA X'
    # Good ol' PCA
    pca = decomposition.PCA(n_components=20)
    # Broken, don't use
    # pca = decomposition.KernelPCA(n_components=20)
    # Sparse PCA
    # pca = decomposition.SparsePCA(n_components=20)
    # Dictionary Learning
    # pca = decomposition.DictionaryLearning(n_components=20)
    # Fast ICA
    # pca = decomposition.FastICA(n_components=20)
    # Factor Analysis
    # pca = decomposition.FactorAnalysis(n_components=20)
    X_reduced = pca.fit_transform(X_normalized)
    print 'Top/Bottom 10 Features:'
    for index, comp in pd.DataFrame(pca.components_, columns=comp_names).iterrows():
        printme = comp
        printme.sort(axis=1, ascending=False)
        print '***********************Component %s***********************'%index
        pp.pprint(printme.ix[:10])
        pp.pprint(printme.ix[-11:-1])
    print 'Cumulative Explained Variance Ratio:'
    print np.cumsum(pca.explained_variance_ratio_[:20])
    # print 'Number of iterations: %s'%pca.n_iter_
    print pd.DataFrame(pca.components_).ix[:0,:].shape
    # hinton(pd.DataFrame(pca.components_).ix[:5,:5].values)
    # plt.title('Component Weights')
    # plt.show()
    if output == True:
    	np.savetxt('./verbal_components.csv', pca.components_, delimiter=',', fmt='%s')
    	np.savetxt('./verbal_expvarratio.csv', pca.explained_variance_ratio_, delimiter=',', fmt='%s')
    	np.savetxt('./verbal_reduced.csv', X_reduced, delimiter=',', fmt='%s')

# msgpack not faster
def msgpack_backup():
    # Try msgpack backup to see if faster load
    # Not faster enough than highest protocol cPickle
    buf = io.BytesIO()
    buf.write(msgpack.packb(X))
    buf.write(msgpack.packb(schema))
    buf.seek(0)
    with open('./verbal_cache.msg', 'wb') as f:
        f.write(buf.read())

# Remove certain features from dataset
def searem(X, string=''):
    clean = []
    for nm in tqdm(list(X.columns)):
        if string not in nm:
            clean.append(nm)
    return(clean)

# If you wanna rebuild the inputs
# build_X(backup=True, export=True)

# Load from pickle cache
print 'Load from verbal_cache.p'
with open('./verbal_cache.p', 'rb') as f:
    X = pickle.load(f)
    schema = pickle.load(f)

# Housekeeping
dt = np.dtype([ (x,'float32') for x in schema ])
X_array = pd.read_csv('./verbal_final.csv')
X_colnames = list(X_array.columns)

no_yelp = searem(X_array, string='yelp')
no_empl_too = searem(X_array[no_yelp], string='empl_')
final_feat = searem(X_array[no_empl_too], string='inc_')
print 'Feature list:'
print final_feat
X_pca = X_array[final_feat]

# Build Indices
pca_index(X_pca.ix[:,1:], missing_values='impute', output=False)

