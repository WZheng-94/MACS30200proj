import json
import pandas as pd
from multiprocessing import Pool
import prawcore
import os.path
import util
import praw

DF = pd.read_csv('full_data.csv')
POST_ID = DF['post_id']
NCHUNKS = 4
REDDITS = util.REDDITS


RANGES = util.find_ranges(POST_ID, NCHUNKS) 
# RANGES = [(10000)]


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
			# sub_dict['replies'] = []
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
		file_name = submit_id + '.json'
		dir_name = "./comments/" + file_name
		if not os.path.isfile(dir_name):
			rv_dict = build_comment_dict(r, submit_id)
			if rv_dict == None:
				print(submit_id)
			else:
				with open(dir_name, 'w', encoding='UTF-8') as fp:
					json.dump(rv_dict, fp)

	return 


ARGS = zip(REDDITS, RANGES)

if __name__ == '__main__':
	with Pool(NCHUNKS) as p:
		p.map(save_comments, ARGS)



