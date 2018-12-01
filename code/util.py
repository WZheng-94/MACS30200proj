import praw
import math

REDDIT_1 

REDDIT_2 

REDDIT_3 

REDDIT_4 

REDDITS = [REDDIT_1, REDDIT_2, REDDIT_3, REDDIT_4]

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

