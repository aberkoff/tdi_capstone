from flask import Flask, render_template, request, url_for, flash, redirect
import pandas as pd
import sqlalchemy as db
from sqlalchemy import text
from werkzeug.exceptions import abort
import os
import postgres_interaction


app = Flask(__name__)

connection_url = os.environ['POSTGRES_CONN']


@app.route('/')
def index():
    return render_template('index.html')
	 
@app.route('/search', methods=('GET', 'POST'))
def search():
	if request.method == 'POST':
		search_val = request.form['search_val']
		search_type = request.form['search_type']
		with engine.connect() as conn:
			data = conn.execute(text(
				"""
				SELECT * FROM book_ids LIMIT 10;
				"""
			))
			conn.commit()
		return render_template('search.html', data=data)
	return render_template('search.html')


	
# if __name__ == "__main__":
# 	connection_url = os.environ['POSTGRES_CONN']
# 	engine = db.create_engine(connection_url, echo=False, future=True)
# 	all_books = pd.read_pickle('../all_books.pkl.tar.gz')
