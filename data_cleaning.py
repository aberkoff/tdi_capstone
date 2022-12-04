import pandas as pd
import re
import numpy as np
import argparse



def get_book_table():
	return pd.read_pickle('all_books.pkl.tar.gz')
	 
def get_related_book_table():
	return pd.read_pickle('related_books.pkl.tar.gz')


def parse_about_pub(pub_list):
	if not pub_list:
		return (np.nan, np.nan)
	pub_info = pub_list[0]
	first_colon = pub_info.find(':')
	last_comma = pub_info.rfind(',')
	publisher = pub_info[first_colon+1: last_comma]
	publisher = publisher.strip()
	try:
		year = re.search('(\d{4})', pub_info[last_comma+1:]).group(0)
	except AttributeError:
		return(np.nan, publisher)
	return (int(year), publisher)
		
# parses raw_book_table['fields.ABOUT.PUBLICATION'] entries
assert parse_about_pub(['[Prince Frederick] : HighBridge Audio, 2020.']) == (2020, 'HighBridge Audio')
assert parse_about_pub(['Ashland : Blackstone Audio, Inc., 2009.']) == (2009, 'Blackstone Audio, Inc.')
assert parse_about_pub(['[New York] : Penguin Random House Audio, [2021]']) == (2021, 'Penguin Random House Audio')
assert parse_about_pub(['Ashland : Blackstone Audio, 2009.', 'Ashland : Blackstone Audio, Inc., 2009.']) == (2009, 'Blackstone Audio')



# parse book length from 'fields.DETAILS.DESCRIPTION' to get length of book in seconds

def get_len_from_notes(notes):
    if pd.api.types.is_list_like(notes):
        for note in notes:
            res = re.search('(\d+):(\d+):(\d+)', note)
            if res is not None:
                hrs = int(res.group(1))*60.0*60.0
                minutes = int(res.group(2))*60.0
                secs = int(res.group(3))
                return hrs + minutes + secs
    return np.NAN

def get_len_from_desc(desc):
	if pd.api.types.is_list_like(desc):
		for d in desc:
			res = re.search('(\d+) hr., (\d+) min., (\d+) sec.', d)
		# print("res = {}".format(res))
			if res is not None:
				hrs = int(res.group(1))*60.0*60.0
				minutes = int(res.group(2))*60.0
				secs = int(res.group(3))
				return hrs + minutes + secs
	return np.NAN
	
assert get_len_from_desc(['1 online resource (1 audio file (03 hr., 09 min., 53 sec.))']) == 11393.0

def get_subjects(subject_field):
	if not pd.api.types.is_list_like(subject_field):
		return np.nan
	else:
		subjects = set()
		for subj in subject_field:
			if subj[-1] == '.':
				subj = subj[:-1].lower()
		subjects.update(s.strip() for s in subj.split(' — '))
		return list(subjects)
		
def get_genres(genre_field):
	if not pd.api.types.is_list_like(genre_field):
		return np.nan
	else:
		genres = set()
		for genre in genre_field:
			if genre[-1] == '.':
				genre = genre[:-1]
		genres.add(genre.lower())
		return list(genres)

def first_list_entry(field, null = np.nan):
  if pd.api.types.is_list_like(field):
    if len(field) == 0:
        return null
    return field[0]
  else:
    return field

def has_multiple_entries(field, num_entries = 2):
    if pd.api.types.is_list_like(field):
        if len(field) >= num_entries:
            return True
    return False
	 
def extract_first_isbn(isbns):
    if pd.api.types.is_list_like(isbns):
            isbn = re.search('\d+', isbns[0])
            if isbn is not None:
                return isbn.group(0)
    return np.NAN
	
def parse_author(author):
	if not author:
		return "Author not listed"
	parts = list(map(str.strip, author.split(',')))
	if len(parts) == 1:
		return parts[0]
	else:
		return " ".join([parts[1], parts[0]])	
	
	 
# fields we want:
# id - the SC number (col 0)
# brief.title - str, 16
# brief.subTitle - str, 17
# brief.primaryLanguage - str, 18
# brief.publicationDate - str, 19
# brief.description - str, 20 summary
# fields.DETAILS.PUBLICATION - list, 27, publisher and date
# fields.DETAILS.CREATORS - list 24, list of authors (but always length 1)
# fields.DETAILS.TITLE - list, 25, includes alternate titles
# fields.DETAILS.SUMMARY - list, 28, multiple summaries
# fields.DETAILS.DESCRIPTION - list, 29 (length)
# fields.CONTRIBUTORS.CONTRIBUTOR_NAME - contributor, 31
# fields.CONTRIBUTORS.CONTRIBUTOR_PERFORMERS - readers, 32
# fields.SUBJECTGENRE.SUBJECT - subjects, 33
# fields.SUBJECTGENRE.GENRE - genres, 34
# fields.IDENTIFIERS.ISBN - list, 37
# fields.CALLCLASS.CALLNO_LC = list, loc call number, 38
# fields.CALLCLASS.CALLNO_DDC = list dewey decimal call number, 39
# 'fields.NOTES.GENERAL', 42, sometimes includes duration



def get_relevant_info_audio(df):
	cleaned = df[['id']].copy()
	cleaned['title'] = df['brief.title'].copy()
	cleaned['subtitle']= df['brief.subTitle'].copy()
	cleaned['language'] = df['brief.primaryLanguage'].copy()
	cleaned['pub_date_listed'] = df['brief.publicationDate'].copy()
	cleaned['description'] = df['brief.description'].copy()
	
	cleaned['author'] = df['fields.DETAILS.CREATORS'].apply(first_list_entry)
	
	cleaned[['publisher_year', 'publisher']] = (pd.DataFrame(df['fields.DETAILS.PUBLICATION']
	.fillna(False).apply(parse_about_pub)
	.tolist(), index=df.index)
	)
	
	# extract book length from either descripiton or notes column, depending on which has it
	cleaned['desc_len'] = df['fields.DETAILS.DESCRIPTION'].apply(get_len_from_desc)
	cleaned['notes_len'] = df['fields.NOTES.GENERAL'].apply(get_len_from_notes)
	cleaned['book_len'] = cleaned[['desc_len', 'notes_len']].max(axis=1)
	cleaned.drop(['desc_len', 'notes_len'], axis=1)
	
	
	cleaned['all_titles'] = df['fields.DETAILS.TITLE'].copy()
	cleaned['all_sumaries'] = df['fields.DETAILS.SUMMARY'].copy()
	cleaned['contributors'] = df['fields.CONTRIBUTORS.CONTRIBUTOR_NAME'].copy()
	cleaned['performers'] = df['fields.CONTRIBUTORS.CONTRIBUTOR_PERFORMERS'].copy()
	
	cleaned['subjects'] = df['fields.SUBJECTGENRE.SUBJECT'].apply(get_subjects)
	cleaned['genres'] = df['fields.SUBJECTGENRE.GENRE'].apply(get_genres)
	
	cleaned['isbns'] = df['fields.IDENTIFIERS.ISBN'].copy()
	cleaned['first_isbn'] = cleaned['isbns'].apply(extract_first_isbn)
	
	cleaned['loc_call_nos'] = df['fields.CALLCLASS.CALLNO_LC'].copy()
	cleaned['dewey_call_nos'] = df['fields.CALLCLASS.CALLNO_DDC'].copy()
	cleaned['general_notes'] = df['fields.NOTES.GENERAL'].copy()
	cleaned['jacket.small'] = df['brief.coverImage.small'].copy()
	cleaned['jacket.medium'] = df['brief.coverImage.medium'].copy()
	cleaned['jacket.large'] = df['brief.coverImage.large'].copy()
	
	 

	return cleaned.drop_duplicates(subset=['id'], keep='first').set_index('id')
	 
def get_relevant_info_related(df):
	return df
	


def main():
	parser = argparse.ArgumentParser()
	parser.add_argument("table_name", 
								help="what kind of table are we cleaning")
	parser.add_argument("input_path",
								help="filename we're reading from")
	parser.add_argument("output_path",
								help="filename we're writing to")
	args = parser.parse_args()
	
	input_table = pd.read_pickle(args.input_path)
	if args.table_name == "book_features":
		cleaned = get_relevant_info_audio(input_table)
	elif args.table_name == "related_books":
		cleaned = get_relevant_info_related(input_table)
	else:
		print("invalid table name. please choose book_features or related_books")
		return
	
	cleaned.to_pickle(args.output_path)




if __name__ == "__main__":
	main()