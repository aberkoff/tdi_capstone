import pandas as pd
import json
from bs4 import BeautifulSoup
import requests
import random
from retry import retry
from datetime import datetime
import logging





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
    availability = [book_info['entities']['bibs'][k]['availability'] for k in book_info['entities']['bibs'].keys()]
    availability_df = pd.json_normalize(availability)


    # book_row = c_bibs[relevant_fields]
    return availability_df
    # except Exception as err:
    #     print("Unexpected {}, {} on book {}".format(err, type(err), book_id))
    #     return (None, None)
    
def scrape_pages(book_ids, first, last):
    availability_list = list()
    id_list = book_ids['id_num']
    
    if last <= 0:
        last = len(book_ids)
    
    for i, book_id in enumerate(id_list[first:last], first):
        # print progress
        if i%100 == 0:
            logging.info('\tscraping availability of book {}: {}'.format(i, book_id))
        try:
            avail = scrape_book_page(book_id)
            availability_list.append(avail)
        except Exception as err:
            logging.warning("Unexpected {}, {} on book {}".format(err, type(err), book_id))

    if availability_list:
        availability = pd.concat(availability_list, ignore_index = True)
        availability.to_pickle("./pickles/availability_{}_{}.pkl".format(first, last))
        


    
    
    
    