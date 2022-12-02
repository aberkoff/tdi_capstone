import pandas as pd
#from selenium import webdriver
import json
import requests
from bs4 import BeautifulSoup
from selenium import webdriver
import geckodriver_autoinstaller
import random
from retrying import retry
#from selenium.webdriver.common.by import By


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
    #print(d)
    # d['author_one'] = authors_list[0]
    # if len(authors_list) >= 2:
    #     d['author_two'] = authors_list[1]
    # if len(authors_list) >=3:
    #     d['author_three'] = authors_list[2]
    return d

@retry(wait_random_min=500, wait_random_max=2000, stop_max_attempt_number=3)
def scrape_page(page_num):
    try:
        #geckodriver_autoinstaller.install()
        #driver = webdriver.Firefox()
        #"https://seattle.bibliocommons.com/v2/search?custom_edit=false&query=formatcode%3A(AB%20)&searchType=bl&suppress=true&f_FORMAT=AB&sort=author&page=1"
        resp=requests.get('https://seattle.bibliocommons.com/v2/search', 
                      params = {'custom_edit': 'false',
                                'query':'formatcode:(AB )',
                                'searchType': 'bl',
                                'suppress':'true',
                                'sort':'author',
                                'page':str(page_num)
                               })
        # driver.get("https://seattle.bibliocommons.com/v2/search?custom_edit=false&query=formatcode%3A(AB%20)&searchType=bl&suppress=true&f_FORMAT=AB&sort=author&page={}".format(page_num))
        soup = BeautifulSoup(resp.content, "html.parser")
        #soup = BeautifulSoup(driver.page_source, "html.parser")
        
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
        print("page: {}".format(page_num))
        print(e)




if __name__ == "__main__":
    for page in range(1,2):
        rows = scrape_page(page)
        with engine.connect() as conn:
            result = conn.execute(
                db.insert(book_ids), rows,
            )
            conn.commit()
    

    
    
          


    