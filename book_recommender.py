from flask import Flask, render_template, request, url_for, flash, redirect
import pandas as pd
import sqlalchemy as db
from sqlalchemy import text
# from werkzeug.exceptions import abort
import os
from postgres_interaction import search_books, get_book_info
from data_cleaning import parse_author


app = Flask(__name__)

@app.context_processor
def process_author():
    return dict(parse_author = parse_author)

@app.route('/')
def index():
    return render_template('index.html')
	 
@app.route('/search', methods=('GET', 'POST'))
def search():
	if request.method == 'POST':
		search_val = request.form['search_val']
		search_type = request.form['search_type']
		# with engine.connect() as conn:
		# 	data = conn.execute(text(
		# 		"""
		# 		SELECT * FROM book_ids LIMIT 10;
		# 		"""
		# 	))
			# conn.commit()
		data = search_books(search_type, search_val)
		return render_template('search.html', data=data)
	return render_template('search.html')



@app.route('/<book_id>')
def book(book_id):
    book_info = get_book_info(book_id)
    return render_template('book.html', data=book_info)
	
# if __name__ == "__main__":
# 	connection_url = os.environ['POSTGRES_CONN']
# 	engine = db.create_engine(connection_url, echo=False, future=True)
# 	all_books = pd.read_pickle('../all_books.pkl.tar.gz')
