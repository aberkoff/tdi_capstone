import pandas as pd
import re
import numpy as np
import argparse
import spacy
nlp = spacy.load("en_core_web_sm")



def fix_audio_errors(df):
	# A children's book called "The Case of The Weird Blue Chicken" whose pubdate is listed as 1917 instead of 2017
	df.at['S30C3286809', 'publisher_year'] = 2017 

def fix_related_errors(df):
	# these 4 books are listed as 'government documents' for some reason...
	df.loc[ df['audioId'] == 'S30C3792290', 'contentType'] = 'NONFICTION' # From a Taller Tower by Seamus McGraw
	df.loc[ df['audioId'] == 'S30C3172736', 'contentType'] = 'NONFICTION' # Last Stand by Seamus Punke
	df.loc[ df['audioId'] == 'S30C3792290', 'contentType'] = 'NONFICTION' # All I Ever Wanted by Kathy Valentine
	df.loc[ df['audioId'] == 'S30C3792290', 'contentType'] = 'FICTION' # Villette by Charlotte Bronte

def fix_checkout_errors(df):
	pass

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

def date_to_int(x):
    if isinstance(x, str) and re.match('\d{4}', x):
        return int(x[:4])
    else:
        return np.nan

def get_intros(performers_field):
    if not pd.api.types.is_list_like(performers_field):
        return np.nan
    intros= set()
    for perf in performers_field:
        lower = perf.lower()
        if lower[-1] == '.':
            lower = lower[:-1]
        ind = lower.find(' by')
        if ind >= 0:
            intro = lower[0:ind+3].strip()
            intros.add(intro)
    return list(intros)

def parse_contributors(contributor_list):
	if not pd.api.types.is_list_like(contributor_list):
		return np.nan
	conts = set()
	for cont in contributor_list:
		if 'overdrive' in cont.lower():
			continue
		ind = cont.find(',')
		if ind >=0:
			fname = cont[ind+1:].strip()
			lname = cont[0:ind].strip()
			name = fname + " " + lname
			conts.add(name)
		else:
			conts.add(cont)
	return list(conts)

	   
def parse_performers(perf_list):
    if not pd.api.types.is_list_like(perf_list):
        return np.nan
    perfs = set()
    for perf in perf_list:
        doc = nlp(perf)
        for ent in doc.ents:
            if ent.label_ == 'PERSON':
                before_char = ''
                before_word = ''
                if ent.start_char > 0:
                    before_char = perf[ent.start_char-1]
                if ent.start_char >=3:
                    before_word = perf[ent.start_char-3:ent.start_char-1].lower()
                if before_word != 'as' and before_char != '(':
                    perfs.add(ent.text)
    return list(perfs)


# parse book length from 'fields.NOTES.GENERAL' to get length of book in seconds
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
	
# parse book length from 'fields.DETAILS.DESCRIPTION' to get length of book in seconds
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

		
bad_entries = {'downloadable audio books', 'audiobooks', 
	'livres audio', 'juvenile sound recordings', 'live sound recordings'
	'sound recordings', 'unabridged audiobooks', 'downloadable audiobooks', 
	'04', '2', '3', '4', '5', 'bk. 1', 'bk. 2', 'bk. 3', 'bk. bne',
	'book 1', 'downloadable video recordings', 'ebook',
	'electronic resource','electronic audio books', 'electronic book',
	'electronic books','electronic resource', 'in literature', 'Internet videos',
 	'Large print books','Large type books', 'sound recordings', 'streaming audio', 'talking books', 'fiction', '', 
	')', '.)'
}
misspellings = {
	'apologetic writings': 'apologetic works',
	'yougn adult fiction': 'young adult fiction', 
	'action and sdventure comics': 'action and adventure comics',
	'autobiographies': 'autobiography',
	'bildsdungsroman': 'bildungsromans',
	'bildungromans': 'bildungsromans',
	'biographies': 'biography',
	'cozy mystery stories': 'cozy mysteries',
	'electornic books': 'electronic books',
	'essays': 'essay',
	'fjuvenile fiction': 'juvenile fiction',
	'fcition': 'fiction',
	'fction': 'fiction',
	'ffiction': 'fiction',
	'ficion': 'fiction',
	'ficition': 'fiction',
	'ficton': 'fiction',
	'fictions': 'fiction',
	'fiction.fiction': 'fiction',
	'first person narratives': 'first person narrative',
	'horror tales': 'horror stories',
	'humerous fiction': 'humorous fiction',
	'juvenilefiction': 'juvenile fiction',
	'juvenile materials': 'juvenile works',
	'juvneile fiction': 'juvenile fiction',
	'legal fiction (literature': 'legal fiction',
	'legal fiction (literature)': 'legal fiction',
	'personal narratives, afghani' : 'personal narratives, afghan',
	'pscyhological fiction' : 'psychological fiction',
	'romantic supsense fiction': 'romantic suspense fiction',
	'romantic suspence fiction': 'romantic suspense fiction',
}

def get_subject_genres(field):
    if not pd.api.types.is_list_like(field):
        return np.nan
    elems = set()
    for elem in field:
        lower = elem.lower()
        if len(lower) == 0:
            continue
        if lower[-1] == '.':
            lower = lower[:-1]
        if lower in misspellings:
            lower = misspellings[lower]
        if lower in bad_entries:
            continue
        separators = ['^', "&lt;delimit&gt;", " — ", "--", "/"]
        separator_found = False
        for sep in separators:
            if sep in lower:
                separator_found = True
                subs = [s.strip() for s in lower.split(sep)]
                elems.update(get_subject_genres(subs))
                break
        if not separator_found:
            elems.add(lower)
    return list(elems)


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

def combine_conts(row):
    keys = ['parsed_contributors', 'parsed_performers']
    return combine_col_lists(row, keys)

def combine_col_lists(row, keys):
    comb = set()
    for key in keys:
        if not pd.api.types.is_list_like(row[key]):
            continue
        comb.update(row[key])
    if comb:
        return list(comb)
    else:
        return np.nan

def populate_field(row, left_field, right_field):
    left_val = row[left_field]
    right_val = row[right_field]
    is_np_nan = isinstance(left_val, float) and np.isnan(left_val)
    if not left_val or is_np_nan:
        if row[right_field] and not(is_np_nan):
            return row[right_field]
    else:
        return row[left_field]
		
def normalize_author(field):
    if isinstance(field, str):
        parts = field.split(',')
        if len(parts) == 1:
            return field
        else:
            fname = parts[1].strip()
            lname = parts[0].strip()
            name = fname + " " + lname
            return name.lower()

def get_relevant_info_audio(df):
	cleaned = df[['id']].copy()
	cleaned['title'] = df['brief.title'].copy()
	cleaned['subtitle']= df['brief.subTitle'].copy()
	cleaned['language'] = df['brief.primaryLanguage'].copy()
	cleaned['pub_date_listed'] = df['brief.publicationDate'].copy()
	cleaned['description'] = df['brief.description'].copy()
	
	cleaned['author'] = df['fields.DETAILS.CREATORS'].apply(first_list_entry)
	cleaned['normalized_author'] = cleaned['author'].apply(normalize_author)
	
	cleaned[['publisher_year', 'publisher']] = (pd.DataFrame(df['fields.DETAILS.PUBLICATION']
	.fillna(False).apply(parse_about_pub)
	.tolist(), index=df.index)
	)
		
	# extract book length from either descripiton or notes column, depending on which has it
	df['desc_len'] = df['fields.DETAILS.DESCRIPTION'].apply(get_len_from_desc) 
	df['notes_len'] = df['fields.NOTES.GENERAL'].apply(get_len_from_notes)
	cleaned['book_len'] = df[['desc_len', 'notes_len']].max(axis=1)
	cleaned['desc_words'] = df['fields.DETAILS.DESCRIPTION'].apply(first_list_entry)
	cleaned['notes_words'] = df['fields.NOTES.GENERAL'].apply(first_list_entry)
	#cleaned.drop(['desc_len', 'notes_len'], axis=1)
	
	
	cleaned['all_titles'] = df['fields.DETAILS.TITLE'].copy()
	cleaned['all_sumaries'] = df['fields.DETAILS.SUMMARY'].copy()
	
	df['parsed_contributors'] = df['fields.CONTRIBUTORS.CONTRIBUTOR_NAME'].apply(parse_contributors)
	df['parsed_performers'] = df['fields.CONTRIBUTORS.CONTRIBUTOR_PERFORMERS'].apply(parse_performers)
	cleaned['parsed_contributors'] = df.apply(combine_conts, axis=1)
	
	cleaned['contributors'] = df['fields.CONTRIBUTORS.CONTRIBUTOR_NAME'].copy()
	cleaned['performers'] = df['fields.CONTRIBUTORS.CONTRIBUTOR_PERFORMERS'].copy()
	
	cleaned['subjects'] = df['fields.SUBJECTGENRE.SUBJECT'].apply(get_subject_genres)
	cleaned['genres'] = df['fields.SUBJECTGENRE.GENRE'].apply(get_subject_genres)
	
	cleaned['isbns'] = df['fields.IDENTIFIERS.ISBN'].copy()
	cleaned['first_isbn'] = cleaned['isbns'].apply(extract_first_isbn)
	
	cleaned['loc_call_nos'] = df['fields.CALLCLASS.CALLNO_LC'].copy()
	cleaned['dewey_call_nos'] = df['fields.CALLCLASS.CALLNO_DDC'].copy()
	cleaned['general_notes'] = df['fields.NOTES.GENERAL'].copy()
	cleaned['jacket.small'] = df['brief.coverImage.small'].copy()
	cleaned['jacket.medium'] = df['brief.coverImage.medium'].copy()
	cleaned['jacket.large'] = df['brief.coverImage.large'].copy()
	
	to_return = cleaned.drop_duplicates(subset=['id'], keep='first').set_index('id')
	fix_audio_errors(to_return)
	return to_return
	 
def get_relevant_info_related(df):
	cleaned = df[['audioId']].copy()
	cleaned['genres'] = df['genreForm'].apply(get_subject_genres)
	cleaned['subjects'] = df['compositeSubjectHeadings'].apply(get_subject_genres)
	cleaned['author'] = df['authors'].apply(first_list_entry)
	cleaned['normalized_author'] = cleaned['author'].apply(normalize_author)
	cleaned['consumptionFormat'] = df['consumptionFormat'].copy()
	cleaned['contentType'] = df['contentType'].fillna('UNDETERMINED')
	cleaned['publicationDate'] = df['publicationDate'].apply(date_to_int)

	cleaned['primaryLanguage'] = df['primaryLanguage'].fillna('UNDETERMINED')
	fields_to_copy = ['id', 'format', 'title', 'subtitle', 
						'description', 'isbns', 'audiences',
						'subjectHeadings', 'description',
						'jacket.small', 'jacket.medium', 'jacket.large', 'rating.averageRating', 'rating.totalCount']
	for field in fields_to_copy:
		cleaned[field] = df[field].copy()
	
	to_return = cleaned.drop_duplicates(subset=['id'], keep='first').set_index('id')
	fix_related_errors(to_return)
	return to_return




def main():
	parser = argparse.ArgumentParser()
	parser.add_argument("table_name", 
								help="which table are we cleaning")
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