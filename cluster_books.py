import pandas as pd
import re
import numpy as np
from sklearn import base
from sklearn.feature_extraction import DictVectorizer
from sklearn.compose import ColumnTransformer
from sklearn.preprocessing import FunctionTransformer
from sklearn.pipeline import Pipeline, FeatureUnion
from sklearn.neighbors import NearestNeighbors
from sklearn.linear_model import RidgeCV, LinearRegression, SGDRegressor, Ridge
from sklearn.decomposition import TruncatedSVD
from sklearn.model_selection import GridSearchCV
from sklearn.metrics import mean_squared_error
from sklearn.model_selection import train_test_split

# this should (eventually) be changed to get
# - ALL the books
# - FROM the sql database
def get_book_table():
    return pd.read_pickle('all_books.pkl.tar.gz')

# ml stuff
class DictEncoder(base.BaseEstimator, base.TransformerMixin):
    
    def __init__(self, col):
        self.col = col
    
    def fit(self, X, y=None):
        return self
    
    def transform(self, X):
        
        def to_dict(l):
            try:
                return {x: 1 for x in l}
            except TypeError:
                return {}
        
        return X[self.col].apply(to_dict)
    

# helper functions to get all the info we want

# parses raw_book_table['fields.ABOUT.PUBLICATION'] entries
assert parse_about_pub(['[Prince Frederick] : HighBridge Audio, 2020.']) == (2020, 'HighBridge Audio')
assert parse_about_pub(['Ashland : Blackstone Audio, Inc., 2009.']) == (2009, 'Blackstone Audio, Inc.')
assert parse_about_pub(['[New York] : Penguin Random House Audio, [2021]']) == (2021, 'Penguin Random House Audio')
assert parse_about_pub(['Ashland : Blackstone Audio, 2009.', 'Ashland : Blackstone Audio, Inc., 2009.']) == (2009, 'Blackstone Audio')

def parse_about_pub(pub_list):
  if not pub_list:
    return ('0000', "none")
  pub_info = pub_list[0]
  first_colon = pub_info.find(':')
  last_comma = pub_info.rfind(',')
  publisher = pub_info[first_colon+1: last_comma]
  try:
    year = re.search('(\d{4})', pub_info[last_comma+1:]).group(0)
  except AttributeError:
    year = '0000'
  publisher = publisher.strip()
  return (int(year), publisher)


# parse book length from 'fields.DETAILS.DESCRIPTION' to get length of book in seconds
assert get_book_length(['1 online resource (1 audio file (03 hr., 09 min., 53 sec.))']) == 11393.0
def get_book_length(desc):
  if pd.api.types.is_list_like(desc):
    res = re.search('(\d+) hr., (\d+) min., (\d+) sec.', desc[0])
    # print("res = {}".format(res))
    if res is not None:
      hrs = int(res.group(1))*60.0*60.0
      minutes = int(res.group(2))*60.0
      secs = int(res.group(3))
      return hrs + minutes + secs
  return np.NAN

def get_subjects(subject_field):
  if not pd.api.types.is_list_like(subject_field):
    return ["unknown"]
  else:
    subjects = set()
    for subj in subject_field:
      if subj[-1] == '.':
        subj = subj[:-1]
      subjects.update(s.strip() for s in subj.split(' — '))
    return list(subjects)

# todo: add grade level info by combining
# 'fields.AUDIENCE.AUDIENCEGENERAL'
# 'fields.AUDIENCE.AUDIENCEGRADE'
# 'fields.AUDIENCE.AUDIENCEREADINGGRADE'

def get_relevant_info(df):
    df[['pub_year', 'publisher']] = (pd.DataFrame(df['fields.ABOUT.PUBLICATION']
                                                .fillna(False).apply(parse_about_pub)
                                                 .tolist(), index=df.index)
                                   )
    df['book_len'] = df['fields.DETAILS.DESCRIPTION'].apply(get_book_length)
    df['subjects'] = df['fields.SUBJECTGENRE.SUBJECT'].apply(get_subjects)
    
    
    df.rename(columns={'fields.DETAILS.CREATORS' : 'authors', 
                       'fields.SUBJECTGENRE.GENRE': 'genres',
                       'fields.CONTRIBUTORS.CONTRIBUTOR_NAME': 'contributors',
                       
                      }, 
              inplace = True)
    return df[['pub_year', 'publisher', 'book_len', 'subjects', 'authors', 'genres', 'contributors']]


def main():
    raw_book_table = get_book_table()
    books_cleaned = get_relevant_info(raw_book_table)
    # relevant = FunctionTransformer(get_relevant_info)
    subject_pipe = Pipeline([('encoder', DictEncoder('subjects')),
                             ('vectorizer', DictVectorizer())
                            ])
    genre_pipe = Pipeline([('encoder', DictEncoder('genres')),
                         ('vectorizer', DictVectorizer())])
    union = FeatureUnion([('categories', subject_pipe),
                          ('genres', genre_pipe)])
    features = genre_pipe.fit_transform(books_cleaned)
    features


if __name__ == "__main__":
    main()