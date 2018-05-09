import pandas as pd
import newspaper
import datetime
import numpy as np
from multiprocessing import Pool
import math
import csv

DF = pd.read_csv('full_data.csv')
DF_TARGET = DF[['post_id', 'url', 'title']]

NCHUNKS = 4

def find_ranges(df,num_chunks):

	ranges = []
	max_size = df.shape[0]
	size = math.ceil(max_size/num_chunks)
	start = 0
	for i in range(num_chunks):
		end = min(start + size, max_size)
		ranges.append((start,end))
		start = end 

	return ranges

RANGES = find_ranges(DF_TARGET, NCHUNKS)

def get_news_data(url):

	print(url)
	default = ('', '', '', '', '')
	article = newspaper.Article(url)
	article.download()
	try:
		article.parse()
		article.nlp()
		author = article.authors
		date = article.publish_date
		if date != None:
			date = date.strftime('%m/%d/%Y')
		text = article.text
		keywords = article.keywords
		summary = article.summary
		return author, date, text, keywords, summary
	except newspaper.article.ArticleException as e:
		return default



def combine_sub_df(ranges):

	up, down = ranges
	for ind, row in DF_TARGET.iloc[up:down, :].iterrows():
		with open('newspaper.csv', 'a') as f:
			writer = csv.writer(f, delimiter=',')
			url = row['url']
			post_id = row['post_id']
			data = list(get_news_data(url))
			data_all = [row['post_id'], row['url'], row['title']] + data
			writer.writerow(data_all)

	return 



if __name__ == '__main__':
	with Pool(NCHUNKS) as p:
		p.map(combine_sub_df, RANGES)
	DF_RAW = pd.read_csv('newspaper.csv')
# 	DF_RAW.columns = ['post_id', 'url', 'title', 'author', 'date', 'text', \
# 					'keywords', 'summary']
# 	DF_RAW = DF_RAW.drop_duplicates()
# 	DF_RAW = DF_RAW[~DF_RAW['text'].isnull()]
# 	DF_RAW['author'] = DF_RAW['author'].apply(lambda x: x.strip("'[]"))
# 	DF_RAW['keywords'] = DF_RAW['keywords'].apply(lambda x: x.strip("'[]"))
# 	DF_RAW.to_csv('newspaper_clean.csv')

	









