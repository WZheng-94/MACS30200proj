import pandas as pd
import newspaper
import datetime
import numpy as np
from multiprocessing import Pool
import math

DF = pd.read_csv('full_data.csv')
URL = DF['url']
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

RANGES = find_ranges(URL, NCHUNKS)

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

	data_list = []
	up, down = ranges
	for url in URL.iloc[up:down]:
		try:
			data = list(get_news_data(url))
		except UnicodeEncodeErro as e:
			data = [''] * 5
		data_list.append(data)	
	data_new = np.array(data_list)
	col = ['author', 'date', 'text', 'keywords', 'summary']
	df = pd.DataFrame(data_new, columns=col)
	df['author'] = df['author'].apply(lambda x: ','.join(x))
	df['keywords'] = df['keywords'].apply(lambda x: ','.join(x))
	


	return df

# args = zip([TITLE] * NCHUNKS, RANGES)
# return pd.concat([DF_TARGET, df], axis = 1)

if __name__ == '__main__':
	with Pool(NCHUNKS) as p:
		p.map(combine_sub_df, RANGES)
	# 	DF_SUB = pd.concat(dfs, axis = 0, ignore_index=True)

	# DF_FINAL = pd.concat([DF_TARGET, DF_SUB], axis = 1, ignore_index=True)
	# DF_FINAL.to_csv('newspaper.csv')







