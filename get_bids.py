# -*- coding: UTF-8 -*-

from elasticsearch import Elasticsearch
import re
import sys
reload(sys)
sys.setdefaultencoding('utf8')

es = Elasticsearch('localhost:9200', timeout=6000 )

fin = open('words','r')
fout = open('id_ans','w')

bid_type = sys.argv[1]
and_start = False
or_words = []
for k in fin:
	line = k.strip().split('\t')
	if line[0] == 'OR':
		minimum_should_match = int(line[1])
		continue
	if line[0] == 'END':
		and_start = True
		continue
	if and_start == False:
		or_words.append(line[0])
		continue

	words_s = []
	for word in or_words:
		ss = {}
		ss["query_string"] = {}
		ss["query_string"]["default_field"] = "biao.content"
		ss["query_string"]["default_operator"] = "or"
		ss["query_string"]["query"] = "\"" + word + "\""
		words_s.append(ss)
	
	words = []
	for word in line:
		ss = {}
		ss["query_string"] = {}
		ss["query_string"]["default_field"] = "biao.content"
		ss["query_string"]["query"] = "\"" + word + "\""
		words.append(ss)
		
	if bid_type > '0':
		ss = {}
		ss["query_string"] = {}
		ss["query_string"]["default_field"] = "biao.type"
		if bid_type == '1':
			ss["query_string"]["query"] = "enter"
		elif bid_type == '2':	
			ss["query_string"]["query"] = "win"
		elif bid_type == '3':	
			ss["query_string"]["query"] = "change"
		words.append(ss)
	words_len = len(words)
	body_str = {"query":{"bool":{"must": words,"should": words_s,"minimum_should_match": minimum_should_match}},"from":0,"size":1,"sort":[],"facets":{}}
	res = es.search(index="lwj2", body = body_str)
	tot = res['hits']['total']
	print tot
	for pos in range(0,tot,1000):
		body_str = {"query":{"bool":{"must": words,"should": words_s,"minimum_should_match": minimum_should_match}},"from":pos,"size":1000,"sort":[],"facets":{}}
		res = es.search(index="lwj2", body= body_str)
		ans_len = len(res['hits']['hits'])
		for i in range(ans_len):
			source = res['hits']['hits'][i]['_source']
			us_id = source['us_id']
			'''
			title = source['title']
			name = source['name']
			'''
			content = source['content']
			dr = re.compile(r'<[^>]+>',re.S)
			content = dr.sub('',content)
			content = content.replace('\r\n','')
			content = content.replace('\r','')
			content = content.replace('\n','')
			content = content.replace('\t','')
			content = content.replace(' ','')	
			content = content.replace('&nbsp','')
			print >> fout, '%s\t%s' % (us_id,content)	
			#print >> fout, '%s\t%s\t%s\t%s' % (us_id,title,name,content)
	print k + "Done"
fout.close()
fin.close()

