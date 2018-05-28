# vars: topic distributions; word count; sentence count; unique word count; 
# average word length
# deri: lexical diversity = unique word count/word count; average sentence length

import pandas as pd 
import gensim
from sklearn.feature_extraction.text import CountVectorizer
import nltk
from collections import Counter
from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer

DF = pd.read_csv('newspaper_clean.csv')
TITLE = DF.title
TEXT = DF.text.tolist()


def get_basic_info(text):

	word_token = nltk.word_tokenize(text)
	token_l = [word.lower() for word in word_token]

	wl = 0
	for word in word_token:
		wl += len(word)
	wd_count = len(word_token)

	avg_wl = wl/wd_count

	porter = nltk.PorterStemmer() 
	uniq_wd_count = len(set([porter.stem(w) for w in token_l]))
	diversity = uniq_wd_count/wd_count

	sent_count = len(nltk.sent_tokenize(text))

	avg_sl = wd_count/sent_count

	tags = nltk.pos_tag(word_token, tagset='universal')
	tag_counts = dict(Counter(tag for word, tag in tags))
	v_tag = tag_counts.get('VERB', 0)/wd_count
	a_tag = tag_counts.get('ADJ', 0)/wd_count  

	return [wd_count, avg_wl, diversity, sent_count, avg_sl, v_tag, a_tag]


def get_all_basic_info():

	data_list = []
	for text in TEXT:
		data = get_basic_info(text)
		data_list.append(data)

	return pd.DataFrame(data_list)


def calculate_topic_dist(num, max_f, min_f):
	'''
	max_f: 0 to 1., 0.2
	min_f: int > 1, 20
	'''

	vect = CountVectorizer(min_df = min_f, max_df = max_f, stop_words = \
						   'english', token_pattern = \
						   '(?u)\\b\\w\\w\\w+\\b').fit(TEXT)
	X = vect.fit_transform(TEXT)
	corpus = gensim.matutils.Sparse2Corpus(X, documents_columns=False)
	id_map = dict((v, k) for k, v in vect.vocabulary_.items())
	ldamodel = gensim.models.ldamodel.LdaModel(corpus, num_topics = num, \
													   id2word = id_map, \
													   passes = 25, \
													   random_state = 34)
	# topics = ldamodel.print_topics(num_topics = num, num_words = 10)
	dist_list = []
	for text in TEXT:
		text_transformed = vect.transform([text])
		corpus = gensim.matutils.Sparse2Corpus(text_transformed, \
											   documents_columns=False)
		topics = ldamodel.get_document_topics(corpus)
		dist_list.append(list(topics)[0])

	results = []
	for data in dist_list:
		rv = dict(data)
		results.append(rv)
	df = pd.DataFrame(results).fillna(0)


	return df

def get_text_sentiment():

	analyzer = SentimentIntensityAnalyzer()
	text_sentiment = TEXT.apply(lambda x: 
								analyzer.polarity_scores(x)['compound'])
	return text_sentiment


def get_title_info():

	analyzer = SentimentIntensityAnalyzer()
	title_sentiment = TITLE.apply(lambda x: \
								  analyzer.polarity_scores(x)['compound'])
	title_len = TITLE.apply(lambda x: len(x.split(' ')))

	return pd.concat([title_sentiment, title_len], axis = 1)


if __name__ == '__main__':
	basic_info = get_all_basic_info()
	topic_dist = calculate_topic_dist(10, 0.2, 20)
	text_sentiment = get_text_sentiment()
	title_info = get_title_info()
	df = pd.concat([basic_info, topic_dist, text_sentiment, title_info])
	columns = [word_count, avg_wordlen, diversity, sent_count, avg_sentl, \
			   v_tag, a_tag, topic_1, topic_2, topic_3, topic_4, topic_5, \
			   topic_6, topic_7, topic_8, topic_9, topic_10, text_sentiment, \
			   title_sentiment, title_len]
	df.columns = columns
	df.to_csv('text_data.csv')









		









