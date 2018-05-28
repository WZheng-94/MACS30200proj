import pandas as pd 
import json
import networkx as nx
import os.path
import numpy as np
import operator

FULL_DATA = pd.read_csv('full_data_clean.csv')
FULL_DATA = FULL_DATA[FULL_DATA['post_id'].apply(lambda x: 
					os.path.exists('comments/' + x + '.json'))]
POST_ID = FULL_DATA['post_id']
FULL_DATA = FULL_DATA.set_index('post_id')
AUTHOR = pd.read_csv('author.csv')
AUTHOR = AUTHOR.set_index('author_id')


def get_edges(post_id):
	'''
	return dcitionary of dictionary of edge information and percentage of 
	missing comment info
	'''

	none_num = 0
	edge_info = {}
	filepath = 'comments/' + post_id + '.json'
	author = FULL_DATA.ix[post_id]['author']
	with open(filepath) as js:
		comment_d = json.load(js)
		comment_num = len(comment_d)

	for key, value in comment_d.items():
		parent = value['parent']
		sender = value['author']
		if sender == 'None':
			none_num += 1
		else:
			if parent != post_id:
				receiver = comment_d[parent]['author']
			else:
				receiver = author

			if receiver != 'None':
				edge_info.setdefault(sender, {})
				edge_info[sender].setdefault(receiver, {'weight':0})
				edge_info[sender][receiver]['weight'] = \
				edge_info[sender][receiver].get('weight', 0) + 1

	try:
		miss_comment_perc = none_num/comment_num
	except:
		miss_comment_perc = 0

	
	return edge_info, miss_comment_perc



def get_main_component(digraph, weak=False):
    
    if weak:
        sub_graphs = list(nx.weakly_connected_component_subgraphs(digraph))
    else:
        sub_graphs = list(nx.strongly_connected_component_subgraphs(digraph))

    if len(sub_graphs) == 0:
    	return None

    main_graph = sub_graphs[0]
    main_graph_num_nodes = main_graph.number_of_nodes()
    for graph in sub_graphs[1:]:
        num_of_nodes = graph.number_of_nodes()
        if num_of_nodes > main_graph_num_nodes:
            main_graph = graph
            main_graph_num_nodes = num_of_nodes

    return main_graph


def add_author_info(graph):
	'''
	return a graph with author information and percentage of nodes with 
	missing info
	'''

	for node in graph.nodes():
		try:
			graph.node[node]['link_karma'], \
			graph.node[node]['comment_karma'] = AUTHOR.loc[node]
		except:
			graph.node[node]['link_karma'] = None
			graph.node[node]['comment_karma'] = None

	d = dict(graph.nodes(data=True))
	none_num = sum(value['link_karma'] == None for value in list(d.values()))

	return graph, none_num/graph.number_of_nodes()

# none info might be related to posts attribute
# compaire all none rate and main none rate then know missing are all outliers


def network_info(post_id): 

	default = [None] * 33
	edge_info, all_miss_perc = get_edges(post_id)

	graph = nx.DiGraph(edge_info)
	all_num_nodes = graph.number_of_nodes()
	all_density = nx.density(graph)
	num_weak_comp = nx.number_weakly_connected_components(graph)
	num_strong_comp = nx.number_strongly_connected_components(graph)

	main_graph = get_main_component(graph)

	if main_graph == None:
		rv =  [post_id, all_miss_perc, all_num_nodes, all_density, num_weak_comp, 
			   num_strong_comp] + [None] * 27
		return rv

	main_graph_full, main_miss_perc = add_author_info(main_graph)

	nodes = dict(main_graph_full.nodes(data=True))
	# edges = dict(main_graph_full.edges(data=True))

	#net work structure

	# get rid of post where main graph less than 10
	main_num_nodes = main_graph_full.number_of_nodes()
	if main_num_nodes < 10:
		rv =  [post_id, all_miss_perc, all_num_nodes, all_density, num_weak_comp, 
			   num_strong_comp, main_num_nodes] + [None] * 26
		return rv
	else:
		main_density = nx.density(main_graph_full)

		main_trans = nx.transitivity(main_graph_full)
		short_path = nx.average_shortest_path_length(main_graph_full, 
			 		 weight='weight') 

		eccen = nx.eccentricity(main_graph_full)
		eccen_val_list = list(eccen.values())
		diameter = np.max(eccen_val_list)
		radius = np.min(eccen_val_list)
		eccen_mean = np.mean(eccen_val_list)
		eccen_std = np.std(eccen_val_list)


		periphery = [key for key, value in eccen.items() if value == diameter]
		peri_lkarma, peri_ckarma = get_eccen_avg(nodes, periphery)
		center = [key for key, value in eccen.items() if value == radius]
		center_lkarma, center_ckarma = get_eccen_avg(nodes, center)

		# node importance
		in_degree_dict = dict(nx.in_degree_centrality(main_graph_full))
		in_degree_lkarma, in_drgree_ckarma = get_top_karma_avg(nodes, 
												               in_degree_dict)
		out_degree_dict = dict(nx.out_degree_centrality(main_graph_full))
		out_degree_lkarma, out_degree_ckarma = get_top_karma_avg(nodes, 
															     out_degree_dict)
		close_dict = dict(nx.closeness_centrality(main_graph_full))
		close_lkarma, close_ckarma = get_top_karma_avg(nodes, close_dict)

		between_dict = dict(nx.betweenness_centrality(main_graph_full, 
							weight='weight', endpoints=True))
		between_lkarma, between_ckarma = get_top_karma_avg(nodes, between_dict)

		hub_dict, auth_dict = nx.hits_numpy(main_graph_full)
		hub_lkarma, hub_ckarma = get_top_karma_avg(nodes, hub_dict)
		auth_lkarma, auth_ckarma = get_top_karma_avg(nodes, auth_dict)

		try:
			author_id = FULL_DATA.loc[post_id]['author']
			author_lkarma, author_ckarma = AUTHOR.loc[author_id]
		except:
			author_lkarma = None
			author_ckarma = None

		rv = [post_id, all_miss_perc, all_num_nodes, all_density, 
			  num_weak_comp, num_strong_comp, main_miss_perc, main_num_nodes, 
			  main_density, main_trans, short_path, diameter, radius, 
			  eccen_mean, eccen_std, peri_lkarma, peri_ckarma, center_lkarma, 
			  center_ckarma, in_degree_lkarma, in_drgree_ckarma, 
			  out_degree_lkarma, out_degree_ckarma, close_lkarma, 
			  close_ckarma, between_lkarma, between_ckarma, hub_lkarma, 
			  hub_ckarma, auth_lkarma, auth_ckarma, author_lkarma, 
			  author_ckarma]

		return rv



def get_top_karma_avg(nodes, target_dict):
	'''
	dict: a dictionary got by nx importance measure
	nodes: a dictionary storing nodes' karma info

	return the average link, comment karma of top 10 percent nodes
	'''

	num = round(len(nodes) * 0.1)
	node_list = sorted(target_dict.items(), key=lambda t: t[1], reverse=True)[:num]
	lkarma = 0
	ckarma = 0
	for node in node_list:
		node_id = node[0]
		try:
			lkarma += nodes[node_id]['link_karma']
			ckarma += nodes[node_id]['comment_karma']
		except:
			num += -1
			if num == 0:
				return None, None 


	return lkarma/num, ckarma/num


def get_eccen_avg(nodes, node_list):
	'''
	nodes: nodes info dict
	list: list of nodes
	'''

	lkarma = [nodes[node]['link_karma'] for node in node_list 
										if nodes[node]['link_karma'] != None]
	ckarma = [nodes[node]['comment_karma'] for node in node_list
										if nodes[node]['comment_karma'] != None]
	if len(lkarma) == 0:
		mean_lkarma = None
	else:
		mean_lkarma = np.mean(lkarma)

	if len(ckarma) == 0:
		mean_ckarma = None
	else:
		mean_ckarma = np.mean(ckarma)

	return mean_lkarma, mean_ckarma

COL = ['post_id', 'perc_miss_comment_author', 'total_num_nodes',
       'total_density', 'num_weak_component', 'num_strong_component',
       'main_miss_node', 'main_num_nodes', 'main_density', 'main_transitivity',
       'main_avg_shortest_path', 'main_diameter', 'main_radius',
       'main_eccentricity_mean', 'main_eccentrisity_std',
       'avg_periphery_lkarma', 'avg_periphery_ckarma', 'avg_center_lkarma',
       'avg_center_ckarma', 'in_degree_top_avg_lkarma',
       'in_degree_top_avg_ckarma', 'out_degree_top_avg_lkarma',
       'out_degree_top_avg_ckarma', 'close_top_avg_lkarma',
       'close_top_avg_ckarma', 'between_top_avg_lkarma',
       'between_top_avg_ckarma', 'hub_top_avg_lkarma', 'hub_top_avg_ckarma',
       'auth_top_avg_lkarma', 'auth_top_avg_ckarma', 'author_lkarma',
       'author_ckarma']


if __name__ == '__main__':
	data_list = []
	for post in POST_ID:
		rv = network_info(post)
		data_list.append(rv)

	df = pd.DataFrame(data_list, columns = COL)
	df.to_csv('comment_data.csv')












