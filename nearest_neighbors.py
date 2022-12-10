import pandas as pd
import re
import numpy as np
import seaborn as sns
import os
from math import floor
import argparse
from sklearn.base import BaseEstimator, RegressorMixin, TransformerMixin
from sklearn.preprocessing import FunctionTransformer, Normalizer, OneHotEncoder
from sklearn.pipeline import Pipeline
from sklearn.feature_extraction import DictVectorizer
from sklearn.compose import ColumnTransformer
from sklearn.decomposition import TruncatedSVD
from sklearn.neighbors import NearestNeighbors

# my stuff
from data_cleaning import combine_col_lists, populate_field



# if we're aggregating a column whose entries are lists, this combines all the lists into one
def merge_lists(x):
    if len(x.dropna())== 0:
        return ['UNDETERMINED']
    comb = set()
    for li in x.dropna():
        comb.update(set(li))
    return list(comb)


# custom function to turn year strings into integers
# (in our dataset, sometimes year strings have trailing characters, e.g. '1991?')
def to_int(x):
    if isinstance(x, str) and re.match('\d{4}', x):
        return int(x[:4])
    else:
        return np.nan



# for a given book X, df has a row for each format the library owns the book in (e.g. audiobook, ebook, physical book)
# this returns a df with one row per book X
def aggregate_related(related):
	
	related['rating.totalRating'] = related['rating.averageRating']*related['rating.totalCount']
	
	# we need a custom to_int because some year strings have trailing characters
	related['publicationDate'] = related['publicationDate'].apply(to_int)
	
	# columns we'll aggregate by merging lists:
	to_merge = ['genres', 'subjects', 'audiences'] 
	# columns where it doesn't matter which row's info we save, so we'll just take the first:
	to_get_first = ['author', 'normalized_author', 'contentType', 'primaryLanguage', 'description',
	'jacket.small', 'jacket.medium', 'jacket.large']
	# in my system, books are more similar if their ORIGINAL publication dates are closer, (audio may be released many years after original book)
	to_minimize=['publicationDate']
	# columns we'll aggregate with a simple sum
	to_sum = ['rating.totalRating', 'rating.totalCount']
	
	agg_dict = dict()

	for key in to_merge:
	    agg_dict[key] = pd.NamedAgg(column=key, aggfunc=merge_lists)
	for key in to_get_first:
	    agg_dict[key] = pd.NamedAgg(column=key, aggfunc='first')
	for key in to_minimize:
	    agg_dict[key] = pd.NamedAgg(column=key, aggfunc=min)
	for key in to_sum:
	    agg_dict[key] = pd.NamedAgg(column=key, aggfunc=sum)

	groups = related.groupby('audioId')
	aggregated = groups.agg(
	    **agg_dict
	)
	
	# recalculate the average rating
	aggregated['rating.averageRating'] = aggregated['rating.totalRating']/aggregated['rating.totalCount']
	
	return aggregated
	

def generate_joined_table(audio, related, output_loc):

	aggregated = aggregate_related(related)
	joined = audio.join(aggregated, how='left', rsuffix='_rel')
	
	obj_pairs = [('language', 'primaryLanguage'),
	             ('description', 'description_rel'),
	             ('author', 'author_rel'),
	             ('normalized_author', 'normalized_author_rel'),
	             ('publisher_year', 'publicationDate'),
	             ('jacket.small', 'jacket.small_rel'),
	             ('jacket.medium', 'jacket.medium_rel'),
	             ('jacket.large', 'jacket.large_rel')
	            ]
	list_pairs = [('subjects', 'subjects_rel'), ('genres', 'genres_rel')]

	for left, right in obj_pairs:
	    joined[left] = joined.apply(lambda x: populate_field(x, left, right), axis=1)

	for left, right in list_pairs:
	    joined[left] = joined.apply(lambda x: combine_col_lists(x, [left, right]), axis=1)
	
	# save our work	
	joined.to_pickle(output_loc) 
	#return joined
	
def get_joined_pickle():
	joined =  pd.read_pickle('pickles/joined.pkl.tar.gz')
	return joined

def fill_list_na(field):
    if pd.api.types.is_list_like(field) and len(field) > 0:
        return field
    else:
        return ['undetermined']	

def prepare_for_ml(joined):
	joined['subject_genres'] = joined.apply(lambda x: combine_col_lists(x, ['subjects', 'genres']), axis=1)
	joined['normalized_author'].fillna('unknown', inplace=True)
	
	mean_year = floor(joined['publisher_year'].mean())
	joined['publisher_year'].fillna(mean_year, inplace=True)
	
	joined['parsed_contributors'] = joined['parsed_contributors'].apply(fill_list_na) # list
	joined['subject_genres'] = joined['subject_genres'].apply(fill_list_na) # list
	joined['audiences'] = joined['audiences'].apply(fill_list_na) # list
	joined['contentType'].fillna('UNDETERMINED', inplace=True)
	
	
	features = ['language', 'normalized_author', 'publisher_year',
	       'parsed_contributors', 'subject_genres', 'audiences', 'contentType']
		   
	return joined[features]
	
def features_list_to_dict(features):
    """
    input: features = a list of strings,
    output: a dict with features as the keys, and all values set to 1
    e.g. ['restaurant', 'cofffee shop', 'drive-thru'] --> {'restaurant':1, 'coffee shop': 1, 'drive-thru':1}
    """

    if pd.api.types.is_list_like(features):
        the_dict = dict()
        for f in features:
            if pd.notnull(f):
                the_dict[f] = 1
        return the_dict
    else:
        return dict()

class DictEncoder(BaseEstimator, TransformerMixin):
    
    def fit(self, X, y=None):
        return self
    
    def transform(self, X):
        if isinstance(X, pd.DataFrame):
            return X.applymap(features_list_to_dict)
        elif isinstance(X, pd.Series):
            return X.apply(features_list_to_dict)
	

def do_ml(X, num_neighbors = 25, neighbors_loc='neighbors.pkl.tar.gz', dists_loc='dists.pkl.tar.gz'):
	encode_and_vectorize = Pipeline([('encode', DictEncoder()), ('vectorize', DictVectorizer())])

	ct = ColumnTransformer([("enc_and_vect_contr", encode_and_vectorize, 'parsed_contributors'),
	                        ("enc_and_vect_s_g", encode_and_vectorize, 'subject_genres'),
	                        ("enc_and_vect_aud", encode_and_vectorize,  'audiences'),
	                        ("one_hot_encode", OneHotEncoder(), ['language', 'normalized_author', 'contentType']),
	                        ("normalize_year", Normalizer(), ['publisher_year'])
	                       ])

	pipe = Pipeline([('transform_columns', ct), ('reduce_dimensions', TruncatedSVD(n_components=100))])
	
	X_trans = pipe.fit_transform(X)
	model = NearestNeighbors(n_neighbors=num_neighbors).fit(X_trans)
	distances, neighbors = model.kneighbors(X_trans)
	
	distances_table = pd.DataFrame(distances, index=X.index)
	distances_table.to_pickle(dists_loc)
	
	neighbor_table_raw = pd.DataFrame(neighbors, index=X.index)
	
	row_nums = pd.Series(X.index)
	def get_sc_num(field):
	    return row_nums[field]
	
	neighbor_table = neighbor_table_raw.applymap(get_sc_num)
	neighbor_table.to_pickle(neighbors_loc)
	

def parse_join(args):
	audio = pd.read_pickle(args.audio)
	related = pd.read_pickle(args.related)
	output_loc = args.output
	generate_joined_table(audio, related, output_loc)
	print("Done. Table saved at {}".format(output_loc))	
	
def parse_find_neighbors(args):
	joined = pd.read_pickle(args.input)
	X = prepare_for_ml(joined)
	do_ml(X, num_neighbors= args.num, neighbors_loc = args.nout, dists_loc = args.dout)
	print("Done.")
	print("neighbors saved at {}".format(args.nout))
	print("distances saved at {}".format(args.dout))


def main():
	parser = argparse.ArgumentParser()
	subparsers = parser.add_subparsers()
	
	join = subparsers.add_parser('join', aliases=['j'], help = "join the audio dataframe with the related works dataframe")
	join.add_argument('--audio', '-a', default = 'datasets/audio.pkl.tar.gz', help = 'location of the pickled audio dataframe')
	join.add_argument('--related', '-r', default = 'datasets/related.pkl.tar.gz', help = 'location of the pickled related dataframe')
	join.add_argument('--output', '-o', default = 'datasets/joined.pkl.tar.gz', help = 'location to write the joined df to')
	join.set_defaults(func=parse_join)
	
	find_neighbors = subparsers.add_parser('find_neighbors', aliases=['f'], help = 'train a k-nearest-neighbor model on dataframe in joined_loc and save result')
	find_neighbors.add_argument('--num', '-n', default='25', type=int, help = 'how many neighbors to find')
	find_neighbors.add_argument('--input', '-i', default = 'datasets/joined.pkl.tar.gz', help = 'where we\'re reading the joined table from')
	find_neighbors.add_argument('--nout', default= "datasets/neighbors.pkl.tar.gz", help = 'where we\'re saving the neighbor table')
	find_neighbors.add_argument('--dout', default= "datasets/dists.pkl.tar.gz", help = 'where we\'re saving the distances table')
	find_neighbors.set_defaults(func=parse_find_neighbors)
	
	args = parser.parse_args()
	args.func(args)
	


if __name__ == "__main__":
	main()
	
	