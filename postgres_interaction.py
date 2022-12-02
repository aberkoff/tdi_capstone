import sqlalchemy as db
from card_catalog_scraping import scrape_page
from sqlalchemy import text
import pandas as pd
import time
#from selenium import webdriver
#import geckodriver_autoinstaller

# engine = db.create_engine("sqlite+pysqlite:///:memory:", echo=True, future=True)
# conn = psycopg2.connect(
#     "postgresql://gbgqpmhx:PUajiD2a9AFI8bDDgMGdn_Sb_SkArTz0@peanut.db.elephantsql.com/gbgqpmhx"
# )


def make_id_table(engine):
    with engine.connect() as conn:
        conn.execute(text(
            """
            DROP TABLE IF EXISTS book_ids
            """
        ))
        # conn.execute(text(
        #     """
        #     CREATE TABLE book_ids (id_num text PRIMARY KEY, 
        #                title text, 
        #                authors text[]);
        #     """
        # ))

        conn.commit()
        
def main():
    engine = db.create_engine("postgresql://gbgqpmhx:PUajiD2a9AFI8bDDgMGdn_Sb_SkArTz0@peanut.db.elephantsql.com/gbgqpmhx", echo=False, future=True)
    make_id_table(engine)
    # metadata_obj = db.MetaData()
    
    #geckodriver_autoinstaller.install()
    #driver = webdriver.Firefox()
    # last page is 5607
    for page_num in range(1, 5608):
        if page_num%100 == 0:
            print("page {}".format(page_num))
        #time.sleep(0.5)
        #df = scrape_page(page_num, driver)
        df = scrape_page(page_num)
        with engine.connect() as conn:
            df.to_sql('book_ids', conn, if_exists = 'append')
            conn.commit()
    driver.quit()
    # with engine.connect() as conn:
    #     df = pd.read_sql_table('book_ids', conn)
        
    # print(df)
        
    

if __name__ == "__main__":
    main()
    


