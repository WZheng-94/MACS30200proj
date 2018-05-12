import pandas as pd 
import os
import json
import csv

COMMENT_DIR = 'comments/'

POST_AUTHOR = pd.read_csv('full_data.csv').author.unique()


def get_one_authors(filename):

	output = json.load(open(filename))
	author_set = set()
	for value in output.values():
		author = value['author']
		author_set.add(author)

	return author_set


def get_all_authors():

	all_authors = set()
	for file in os.listdir(COMMENT_DIR):
		filename = 'comments/' + os.fsdecode(file)
		if filename != 'comments/.DS_Store':
			author_set = get_one_authors(filename)
			all_authors = all_authors.union(author_set)

	return all_authors


if __name__ == '__main__':
	ALL_AUTHORS = get_all_authors()
	ALL_AUTHORS = ALL_AUTHORS.union(set(POST_AUTHOR))
	print(len(ALL_AUTHORS))
	with open('author.json', 'w') as f:
		json.dump(list(ALL_AUTHORS), f)



