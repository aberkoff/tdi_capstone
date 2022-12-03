import sqlalchemy as db
from sqlalchemy import text, MetaData
import pandas as pd
import time
import os
import logging
import argparse


def drop_table(engine, table_name):
    with engine.connect() as conn:
        conn.execute(text(
            """
            DROP TABLE IF EXISTS {}
            """.format(table_name)
        ))
        
        
def add_record(engine, df, table_name):
    with engine.connect() as conn:
        df.to_sql(table_name, conn, if_exists = 'append')
        conn.commit()
        
def read_head(engine, table, head_len = 10):
	with engine.connect() as conn:
		select_statement = table.select().fetch(head_len)
		result_set = conn.execute(select_statement)
		for r in result_set:
			print(r)
		conn.commit()

def get_book_ids(engine):
	with engine.connect() as conn:
		logging.info('reading book_ids into a dataframe')
		book_ids = pd.read_sql_table('book_ids', conn)
		# book_ids.to_pickle("./book_ids.pkl")
		conn.commit()
	return book_ids

	

def get_db_engine():
	connection_url = os.environ['POSTGRES_CONN']
	try:
		logging.info('connecting to the database.')
		engine = db.create_engine(connection_url, echo=False, future=True)
		return engine
	except Exception as e :
		logging.warning("Exception: {} of type: {} when connecting to the database".format(e, type(e)))



table_names = ['book_ids', 'book_features', 'related_books']


def get_valid_table_name(name):
	while name not in(table_names):
		print("that is not a table in our database. our tables are:")
		print("\n".join(table_names))
		name = input("please select a table: ")
	print("thank you. you have selected: {}".format(name))
	return name

def main():
	parser = argparse.ArgumentParser()
	group = parser.add_mutually_exclusive_group()	
	group.add_argument("-r", "--read", help="read sql table", action="store_true")
	group.add_argument("-w", "--write", help="write sql table", action="store_true")
	group.add_argument("-d", "--delete", help="delete sql table", action="store_true")
	parser.add_argument("table_name", help="the sql table name")

	args = parser.parse_args()
	logfile = 'logs/postgres_interaction.log'
	
	logging.basicConfig(filename=logfile, 
								level=logging.INFO,
								format = '%(asctime)s %(message)s'
								)
	table_name = get_valid_table_name(args.table_name)
	engine = get_db_engine()
	metadata_obj = MetaData()
	metadata_obj.reflect(bind=engine)
	
	if args.write:
		logging.info('writing to table {}'.format(table_name))
		filename = input("enter path for the pickled dataframe to write to {}:\n".format(table_name))
		df = pd.read_pickle(filename)
		add_record(engine, df, table_name)
	elif args.delete:
		logging.info('confirming deletion of table {}'.format(table_name))
		yes_no = input('are you sure you want to delete {}? y/n: '.format(table_name))
		if lower(yes_no) in {'y', 'yes'}:
			drop_table(engine, book_ids)
	elif args.read:
		logging.info('reading from table {}'.format(table_name))
		table = metadata_obj.tables[table_name]
		read_head(engine, table)
	else:
		logging.info('no option selected')
		print('no valid option selected. please run again.')
				

	logging.info("done")
	logging.info("-------------------")


if __name__ == "__main__":
    main()
	
    



