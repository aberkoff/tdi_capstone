import pandas as pd
import json
from bs4 import BeautifulSoup
import requests
import random
from retry import retry
from datetime import datetime
import argparse
import logging
from postgres_interaction import get_db_engine, get_book_ids

@retry(exceptions=Exception, tries=2, delay=2, 
       max_delay=None, backoff=1, jitter=(1,10))
def get_availability(book_id):
    # try:
    page_url = 'https://seattle.bibliocommons.com/v2/record/{}'.format(book_id)
    resp = requests.get(page_url)
    soup = BeautifulSoup(resp.content, "html.parser")
    book_info_json = soup.find('script', attrs={'type':'application/json',
                                           'data-iso-key':'_0'
                                      })
    book_info = json.loads(book_info_json.text)
    avail_dict = book_info['entities']['bibs'][book_id]['availability']
    
    return_dict = dict()
    
    return_dict['available'] = avail_dict['availableCopies']
    return_dict['total'] = avail_dict['totalCopies']
    return_dict['held'] = avail_dict['heldCopies']
    return_dict['on_order'] = avail_dict['onOrderCopies']
    return return_dict


@retry(exceptions=Exception, tries=5, delay=2, 
       max_delay=None, backoff=1, jitter=(1,10), logger=logging.getLogger(__name__))
def scrape_book_page(book_id):
    # try:
    page_url = 'https://seattle.bibliocommons.com/v2/record/{}'.format(book_id)
    resp = requests.get(page_url)
    soup = BeautifulSoup(resp.content, "html.parser")
    book_info_json = soup.find('script', attrs={'type':'application/json',
                                           'data-iso-key':'_0'
                                      })
    book_info = json.loads(book_info_json.text)
    related_books = [book_info['entities']['bibs'][k]['briefInfo'] for k in book_info['entities']['bibs'].keys()]
    related_books_df = pd.json_normalize(related_books)
    related_books_df['audioId'] = book_id

    catalog_info = book_info['entities']['catalogBibs'][book_id]
    catalog_info['fields'] = custom_normalize(catalog_info['fields'])
    c_bibs = pd.json_normalize(catalog_info)

    return (related_books_df, c_bibs)

    
def scrape_pages(engine, first, last):
	book_ids = get_book_ids(engine)
	related_list = list()
	features_list = list()
	id_list = book_ids['id_num']
	
	if last < 0:
		last = len(book_ids)
    
	for i, book_id in enumerate(id_list[first:last], first):
		if i%100 == 0:
			logging.info('\tscraping book {}: {}'.format(i, book_id))
		try:
			rel, info = scrape_book_page(book_id)
			related_list.append(rel)
			features_list.append(info)
		except Exception as err:
			logging.warning("Unexpected {}, {} on book {}".format(err, type(err), book_id))
	if related_list:
		related = pd.concat(related_list, ignore_index = True)
		related.to_pickle("./pickles/related_titles_{}_{}.pkl".format(first, last))
	if features_list:
		features = pd.concat(features_list, ignore_index = True)
		features.to_pickle("./pickles/features_{}_{}.pkl".format(first, last))
        
def custom_normalize(the_list):
    the_dict = dict()
    for item in the_list:
        key_0 = item['category']
        list_0 = item['items']
        dict_0 = dict()
        for item_0 in list_0:
            key_1 = item_0['fieldName']
            list_1 = item_0['fieldValues']
            list_2 = list()
            for ind, item_1 in enumerate(list_1):
                if item_1['primary']:
                    list_2.extend(item_1['primary']['values'])
                if item_1['secondary']:
                    list_2.extend(item_1['secondary']['values'])
            dict_0[key_1] = list_2
        the_dict[key_0] = dict_0
    return the_dict
        
def main():
	parser = argparse.ArgumentParser()
	parser.add_argument("start", type=int,
								help="where to start scraping")
	parser.add_argument("end", type=int,
								help="where to stop scraping")
	args = parser.parse_args()
	logfile = 'logs/book_page_scraping.log'
	logging.basicConfig(filename=logfile, 
								level=logging.INFO,
								format = '%(asctime)s %(message)s'
								)
								
	logging.info("scraping books # {} through {}...".format(args.start, args.end))
	engine = get_db_engine()
	scrape_pages(engine, args.start, args.end)
	logging.info("done")
	logging.info("-------------------")


if __name__ == "__main__":
    main()
    
    
    
    