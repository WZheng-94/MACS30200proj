import pandas as pd 
import praw 
import csv
from time import sleep
import prawcore
import math
from multiprocessing import Pool

DF = pd.read_csv('reddit_worldnews_start_to_2016-11-22.csv')
FILTER = ((DF['date_created'].str.startswith('2015')) |  \
			(DF['date_created'].str.startswith('2014'))) & \
			(DF['up_votes'] > 50)
DF_TARGET = DF[FILTER]
TITLE = DF_TARGET['title']
print(TITLE.shape)
# NCHUNKS = 4

# def find_ranges(df,num_chunks):

# 	ranges = []
# 	max_size = df.shape[0]
# 	size = math.ceil(max_size/num_chunks)
# 	start = 0
# 	for i in range(num_chunks):
# 		end = min(start + size, max_size)
# 		ranges.append((start,end))
# 		start = end 

# 	return ranges

# RANGES = find_ranges(TITLE, NCHUNKS)


REDDIT = praw.Reddit(client_id = 'Ip1jf64PVY6_QQ' , 
					 client_secret = 'qIBcz5jWTmcqs5TpPM9SDpmKYlo', 
					 username = 'ErWeiZheng', 
					 password= 'TesanZheng1006', 
					 user_agent='praw_trial')

def get_post_info(title):

	default = (None, None, title, None, None, None, None, None, None)
	try:
		search_list = list(REDDIT.subreddit('worldnews').search(title))
	except prawcore.exceptions.ServerError as e:
		sleep(5)
		# search_list = list(REDDIT.subreddit('worldnews').search(title))
		print('empty')
		return default
	except prawcore.exceptions.Redirect as e:
		sleep(5)
		# search_list = list(REDDIT.subreddit('worldnews').search(title))
		print('empty')
		return default
	except prawcore.exceptions.RequestException as e:

		sleep(5)
		# search_list = list(REDDIT.subreddit('worldnews').search(title))
		print('empty')
		return default


	if len(search_list) > 0:
		post_id = search_list[0].id
		submit = REDDIT.submission(id=post_id)
		score = submit.score	
		url = submit.url
		ups = submit.ups
		downs = submit.downs
		author = submit.author.name
		time = submit.created_utc
		num_comments = submit.num_comments

		return post_id, url, title, ups, downs, score, author, time, num_comments
	
	else:
		return default

def append_data():

	# df = args[0]
	# ranges = args[1]
	# up, down = ranges
	# num = 0
	with open('basic_data.csv', 'a') as f:
		writer = csv.writer(f, delimiter=',')
		# writer.writerow(['post_id', 'url', 'title', 'ups', 'downs', 'score', \
		# 				'author', 'time', 'num_comments'])
		for title in TITLE.iloc[1208:]:
			data = get_post_info(title)
			if data:
				writer.writerow(list(data))
				# num += 1
				# print(num)
	return

if __name__ == '__main__':
	append_data()
	DF_RAW = pd.read_csv('basic_data.csv').drop_duplicates()
	DF_FULL = DF_RAW[~DF_RAW.post_id.isnull()] # 22702
	DF_EMPTY = DF_RAW[DF_RAW.post_id.isnull()] # 4877
	DF_FULL.to_csv('full_data.csv', index = False)
	DF_EMPTY.to_csv('empty_data.csv', index = False)

# args = zip([TITLE] * NCHUNKS, RANGES)

# if __name__ == '__main__':
# 	with Pool(NCHUNKS) as p:
# 		p.map(append_data, args)





