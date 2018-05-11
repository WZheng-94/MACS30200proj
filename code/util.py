import praw
import math

REDDIT_1 = praw.Reddit(client_id = 'Ip1jf64PVY6_QQ', 
						client_secret = 'qIBcz5jWTmcqs5TpPM9SDpmKYlo', 
						username = 'ErWeiZheng', 
						password= 'TesanZheng1006', 
						user_agent='praw_trial')

REDDIT_2 = praw.Reddit(client_id = 'FGVEIcwf1MNiIw', 
						client_secret = '8cTiak9cFTx0s0W4EOMOg_o7Bag', 
						username = 'ErWeiZheng', 
						password= 'TesanZheng1006', 
						user_agent='praw_trial_2')

REDDIT_3 = praw.Reddit(client_id = 'BclBM7ciAkAEqA', 
						client_secret = 'gDXfV5-Co0To-GRUdtdIJqB0qUY', 
						username = 'ErWeiZheng', 
						password= 'TesanZheng1006', 
						user_agent='praw_trial_3')

REDDIT_4 = praw.Reddit(client_id = 'SzuNTymQSs0jQg', 
						client_secret = 'Q1Y0IF9O0nT62LS2vj5YuNgAdBU', 
						username = 'ErWeiZheng', 
						password= 'TesanZheng1006', 
						user_agent='praw_trial_4')

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

