import praw
import json
import pandas as pd
import math
from multiprocessing import Pool
import prawcore

DF = pd.read_csv('full_data.csv')
POST_ID = DF['post_id']
NCHUNKS = 4


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


RANGES = find_ranges(POST_ID, NCHUNKS) 


def build_comment_dict(r, submit_id):

	submission = r.submission(submit_id)
	try: 
		pre_comments = submission.comments.list()
	except prawcore.ResponseException as e:
		return None
	comments = get_all_comment(pre_comments)
	rv_dict = {}
	for comment in comments:
		label = comment.id
		if label not in rv_dict:
			sub_dict = {}
			sub_dict['author'] = str(comment.author)
			sub_dict['replies'] = []
			# if comment.body == '[deleted]':
			# 	sub_dict['deleted'] = True
			# else:
			# 	sub_dict['deleted'] = False
			sub_dict['body'] = comment.body
			sub_dict['parent'] = str(comment.parent())
			rv_dict[label] = sub_dict

	return rv_dict


def get_all_comment(comments):

	comment_list = []
	for comment in comments:
		if not type(comment) is praw.models.reddit.more.MoreComments:
			comment_list.append(comment)
		else:
			children = comment.comments()
			if len(children) != comment.count:
				sub_comments = get_all_comment(children)
				comment_list = comment_list + sub_comments

	return comment_list


def save_comments(args):

	r, ranges = args
	up, down = ranges
	for submit_id in POST_ID.iloc[up: down]:
		rv_dict = build_comment_dict(r, submit_id)
		if rv_dict == None:
			print(submit_id)
		else:
			file_name = submit_id + '.json'
			dir_name = "./comments/" + file_name
			with open(dir_name, 'w', encoding='UTF-8') as fp:
				json.dump(rv_dict, fp)

	return 


ARGS = zip(REDDITS, RANGES)

if __name__ == '__main__':
	with Pool(NCHUNKS) as p:
		p.map(save_comments, ARGS)



