from flask import Flask, render_template, request, url_for, flash, redirect
import pandas as pd
import sqlalchemy as db
from sqlalchemy import text
# from werkzeug.exceptions import abort
import os

from postgres_interaction import search_books, get_book_info, get_neighbors, get_neighbors_info
from data_cleaning import parse_author
from book_page_scraping import get_availability



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
		data = search_books(search_type, search_val)
		return render_template('search.html', data=data)
	return render_template('search.html')

@app.route('/<book_id>', methods=('GET', 'POST'))
def book(book_id):
	book_info = get_book_info(book_id)
	availability = get_availability(book_id)
	if request.method == 'POST':
		nearest = get_neighbors(book_id)
		neighbors = get_neighbors_info(nearest)
		return render_template('book.html', data=book_info, neighbors=neighbors, availability = availability)
	return render_template('book.html', data=book_info, availability=availability)
	
	
	
# if __name__ == "__main__":
# 	connection_url = os.environ['POSTGRES_CONN']
# 	engine = db.create_engine(connection_url, echo=False, future=True)
# 	all_books = pd.read_pickle('../all_books.pkl.tar.gz')
