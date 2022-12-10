import sqlalchemy as db
from sqlalchemy import text, MetaData
import pandas as pd
import os
import logging
import argparse
import sys


def get_neighbors_info(nearest):
	nearest_tuple = tuple(nearest)
	table = metadata_obj.tables['book_features']
	sc_num = table.c['id']
	icon_url = table.c['jacket.small']
	title = table.c['title']
	subtitle = table.c['subtitle']
	author = table.c['author']
	query = db.select(sc_num, icon_url, title, subtitle, author).where(text("id IN {id_tuple}".format(id_tuple=nearest_tuple)))
	with engine.connect() as conn:
		result = conn.execute(query)
		conn.commit()
	return result

def get_neighbors(sc_num, num_neighbors=25):
	table = metadata_obj.tables['neighbors']
	sc_num_col = table.c['id']
	results_cols = map(lambda x: table.c[x], map(str, list(range(1,num_neighbors))))
	select_statement = db.select(*results_cols).where(sc_num_col == sc_num).fetch(1)
	with engine.connect() as conn:
		result = conn.execute(select_statement)
		conn.commit()
	return list(result)[0]
	

def search_books(search_type, search_val, table_name='book_features'):
	table = metadata_obj.tables[table_name]
	sc_num = table.c['id']
	icon_url = table.c['jacket.small']
	title = table.c['title']
	subtitle = table.c['subtitle']
	author = table.c['author']
	
	if search_type == 'title':
		select_statement = db.select(sc_num, icon_url, title, subtitle, author).where(title.match(search_val)).fetch(10)
	elif search_type == 'author':
		select_statement = db.select(sc_num, icon_url, title, subtitle, author).where(author.match(search_val)).fetch(10)
	elif search_type == 'narrator':
		select_statement = db.select(sc_num, icon_url, title, subtitle, author).fetch(10)
	elif search_type == 'sc_number':
		select_statement = db.select(sc_num, icon_url, title, subtitle, author).where(sc_num == search_val)
	else:
		return None
	with engine.connect() as conn:
		result_set = conn.execute(select_statement) # note: this is an iterable!!!!
		conn.commit()
	return result_set
		
def get_book_info(book_id):
	table = metadata_obj.tables['book_features']
	sc_num = table.c['id']
	select_statement = db.select(table).where(sc_num == book_id)
	with engine.connect() as conn:
		result_set = conn.execute(select_statement) # note: this is an iterable!!!!
		conn.commit()
	return result_set.first()

	
def drop_table(table_name):
	with engine.connect() as conn:
		conn.execute(text(
			"""
			DROP TABLE IF EXISTS {}
			""".format(table_name)
		))
		conn.commit()
        
        
def add_table(df, table_name):
    with engine.connect() as conn:
        df.to_sql(table_name, conn, if_exists = 'append')
        conn.commit()
        
def read_head(table, head_len = 10):
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




def parse_read(args):
	table_name = args.table_name
	print("reading the first {} rows of {}...\n".format(args.length, table_name))
	table = metadata_obj.tables[table_name]
	read_head(table, args.length)
	
def parse_write(args):
	table_name = args.table_name
	print("writing {} to {}...\n".format(args.filename, table_name))
	# table may not exist yet...
	# table = metadata_obj.tables[table_name]
	df = pd.read_pickle(args.filename)
	add_table(df, table_name)
	
def parse_delete(args):
	table_name = args.table_name
	table = metadata_obj.tables[table_name]
	logging.info('confirming deletion of table {}'.format(table_name))
	yes_no = input('are you sure you want to delete {}? y/n: '.format(table_name))
	if lower(yes_no) in {'y', 'yes'}:
		print('dropping table {}'.format(table_name))
		drop_table(table)

def parse_search(args):
	result = search_books(args.by, args.value, args.table_name)
	args.outfile.write(str(list(result)))
	
def parse_neighbors(args):
	neighbors = get_neighbors(args.sc_num)
	result = get_neighbors_info(neighbors)
	for res in result:
		args.outfile.write(str(res))
	
	
	
	


table_names = ['book_ids', 'book_features', 'related_books', 'neighbors', 'joined']


engine = get_db_engine()
metadata_obj = MetaData()
metadata_obj.reflect(bind=engine)

def main():
	parser = argparse.ArgumentParser()
	subparsers = parser.add_subparsers()
	
	read = subparsers.add_parser('read', aliases=['r'], help = "read the first [LENGTH] entries of [table_name]")
	read.add_argument('table_name', choices=table_names, help = 'name of table to read from')
	read.add_argument('--length', '-l', default='10', type=int, help = 'how many entries to read, default = 10')
	read.set_defaults(func=parse_read)
	
	write = subparsers.add_parser('write', aliases=['w'], help = "write a pickled pandas dataframe to an sql table")
	write.add_argument('filename', help = 'name of pickled pandas dataframe')
	write.add_argument('table_name', choices = table_names, help = 'name of table to read from')
	write.set_defaults(func=parse_write)
	
	delete = subparsers.add_parser('delete', aliases=['d'], help = "drop an sql table")
	delete.add_argument('table_name', choices = table_names, help = 'name of table to drop')
	delete.set_defaults(func=parse_delete)
	
	neighbors = subparsers.add_parser('neighbors', aliases=['n'], help = "query neighbors")
	neighbors.add_argument('sc_num', help = 'the sc_num of the book you are querying')
	neighbors.add_argument('outfile', nargs='?', type=argparse.FileType('w'), default=sys.stdout, help = "where to print neighbors")
	neighbors.set_defaults(func=parse_neighbors)

	search = subparsers.add_parser('search', aliases=['s'], help = "search an sql table")
	search.add_argument('table_name', choices = table_names, help = 'name of table to search')
	search.add_argument('--by', 
						choices = ['sc_number', 'title', 'author'],
						default='sc_number', 
						help = 'what to search by (default = sc_number)'
						)
	search.add_argument('--value', '-v', help = "value to search for", required = True)
	search.add_argument('outfile', nargs='?', type=argparse.FileType('w'), default=sys.stdout, help = "where to print search results")
	search.set_defaults(func=parse_search)
	
	



	args = parser.parse_args()
	args.func(args)
	logfile = 'logs/postgres_interaction.log'
	
	logging.basicConfig(filename=logfile, 
								level=logging.INFO,
								format = '%(asctime)s %(message)s'
								)

	# elif args.search:
	# 	logging.info('searching {}'.format(table_name))
	# 	table = metadata_obj.tables[table_name]
	# 	read_head(engine, table)
	# else:
	# 	logging.info('no option selected')
	# 	print('no valid option selected. please run again.')
				

	logging.info("done")
	logging.info("-------------------")


if __name__ == "__main__":
    main()
	
engine = get_db_engine()
metadata_obj = MetaData()
metadata_obj.reflect(bind=engine)
    



