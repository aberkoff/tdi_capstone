import pandas as pd
import json
import requests
from bs4 import BeautifulSoup
import random
from retrying import retry
import argparse
import logging
import postgres_interaction



def book_tuple(k, v):
    briefInfo = v["briefInfo"]
    return (k, briefInfo['title'], briefInfo['authors'])

def book_dict(k, v):
    d = dict()
    briefInfo = v["briefInfo"]
    d['id_num'] = k
    d['title'] = briefInfo['title']
    authors_list = briefInfo['authors']
    d['authors'] = authors_list
    return d

@retry(wait_random_min=500, wait_random_max=2000, stop_max_attempt_number=3)
def scrape_results_page(page_num):
    try:
        resp=requests.get('https://seattle.bibliocommons.com/v2/search', 
                      params = {'custom_edit': 'false',
                                'query':'formatcode:(AB )',
                                'searchType': 'bl',
                                'suppress':'true',
                                'sort':'author',
                                'page':str(page_num)
                               })

        soup = BeautifulSoup(resp.content, "html.parser")
        book_info_json = soup.find('script', attrs={'type':'application/json',
                                           'data-iso-key':'_0'
                                      })
        book_info = json.loads(book_info_json.text)
        book_list = book_info['entities']['bibs']
        book_dicts = list() 

        for k, v in book_list.items():
            book_dicts.append(book_dict(k,v))

        df = pd.DataFrame(book_dicts)
        df.set_index('id_num', inplace = True)
        
        return df
    except requests.exceptions.RequestException as e:  # This is the correct syntax
        logging.warning("Unexpected {}, {} on page {}".format(e, type(e), page_num))


def scrape_card_catalog(engine, start, end):
    # last page is 5607
	if end < 0:
		end = 5608
	for page_num in range(start, end):
		if page_num%100 == 0:
			logging.info("page {}".format(page_num))
		df = scrape_results_page(page_num)
		postgres_interaction.add_record(engine, df, 'book_ids')


def main():
	parser = argparse.ArgumentParser()
	parser.add_argument("start", type=int,
								help="which result page to start start scraping")
	parser.add_argument("end", type=int,
								help="which results page to stop scraping")
	args = parser.parse_args()
	logfile = 'logs/card_catalog_scraping.log'
	logging.basicConfig(filename=logfile, 
								level=logging.INFO,
								format = '%(asctime)s %(message)s'
								)
								
	logging.info("scraping catalog pages # {} through {}...".format(args.start, args.end))
	engine = postgres_interaction.get_db_engine()
	scrape_card_catalog(engine, args.start, args.end)
	logging.info("done")
	logging.info("-------------------")


if __name__ == "__main__":
    main()
          


    