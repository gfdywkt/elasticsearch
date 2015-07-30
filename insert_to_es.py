#!/usr/bin/python -u
# -*- coding: UTF-8 -*-

from elasticsearch import Elasticsearch
import MySQLdb
from datetime import datetime
import pyes

import sys
reload(sys)
sys.setdefaultencoding('utf8')

if sys.stdout.encoding is None: 
    import codecs 
    writer = codecs.getwriter("utf-8") 
    sys.stdout = writer(sys.stdout)


def get_data(uid):
	conn = MySQLdb.Connect(host='myhost', user='myuser', passwd='mypasswd', db='mydb', charset='utf8')
	cur = conn.cursor()
	start_num = str(uid)
	end_num = str(uid + 100000)
	sqlStr = "SELECT site, type, province, city, url, number, title, name, date, start, end, crawled, content, us_id, company FROM crawl2 where us_id >= "+ start_num +" and us_id < "+ end_num
	cur.execute(sqlStr)
	data = list(cur.fetchall())
	return data

def es_insert(conn, site, typee, province, city, url, number, title, name, date, start, end, crawled, content, us_id, company):
	doc = {
		'site': site,
		'type': typee,
		'province': province,
		'city': city,
		'url': url,
		'number': number,
		'title': title,
		'name': name,
		'date': date,
		'start': start,
		'end': end,
		'crawled': crawled,
        'content': content,
		'us_id': us_id,
		'company': company,
        'timestamp': datetime.utcnow()
    }
	conn.index(doc,'lwj2','biao', us_id)
	#es.index(index="lwj", doc_type='biao', id=us_id, body=doc)

if __name__ == "__main__":
	#es = Elasticsearch('localhost:9200')
	conn = pyes.ES('localhost:9200')
	'''
	try:
		conn.indices.delete_index('lwj2')
	except:
		pass
	conn.indices.create_index('lwj2')
	'''
	mapping = {u'site': {'boost': 1.0,                 
                    'index': 'not_analyzed',
                    'store': 'yes',
                    'type': u'string'},
			   u'type': {'boost': 1.0,
					'index': 'not_analyzed',
					'store': 'yes',
					'type': u'string'},
               u'province': {'boost': 1.0,                 
                    'index': 'not_analyzed',
                    'store': 'yes',
                    'type': u'string'},
               u'city': {'boost': 1.0,                 
                    'index': 'not_analyzed',
                    'store': 'yes',
                    'type': u'string'},
			   u'url': {'boost': 1.0,
					'index': 'not_analyzed',
					'store': 'yes',	
					'type': u'string'},
			   u'number': {'boost': 1.0,
					'index': 'not_analyzed',
					'store': 'yes',	
					'type': u'string'},
			   u'title': {'boost': 1.0,
                    'index': 'analyzed',
                    'store': 'yes',
                    'type': u'string',
                    'analyzer': 'ik'},
			   u'name': {'boost': 1.0,
					'index': 'not_analyzed',
					'store': 'yes',	
					'type': u'string'},
               u'date': {'boost': 1.0,                 
                    'index': 'not_analyzed',
                    'store': 'yes',
                    'type': u'string'},
               u'start': {'boost': 1.0,                 
                    'index': 'not_analyzed',
                    'store': 'yes',
                    'type': u'string'},
               u'end': {'boost': 1.0,                 
                    'index': 'not_analyzed',
                    'store': 'yes',
                    'type': u'string'},
               u'crawled': {'boost': 1.0,                 
                    'index': 'not_analyzed',
                    'store': 'yes',
                    'type': u'string'},
               u'content': {'boost': 1.0,                 
                    'index': 'analyzed',
                    'store': 'yes',
                    'type': u'string',
                    'analyzer': 'ik'},
			   u'us_id':{'boost': 1.0,
					'index': 'not_analyzed',
					'store': 'yes',
					'type': u'string'},
               u'company': {'boost': 1.0,                 
                    'index': 'analyzed',
                    'store': 'yes',
                    'type': u'string',
                    'analyzer': 'ik'},
               u'timestamp': {'boost': 1.0,
                    'index': 'not_analyzed',
                    'store': 'yes',
                    'type': u'string'},
    }
	conn.indices.put_mapping('biao', {'properties':mapping}, ['lwj2'])
	uid = 1
	while True:
		datas = get_data(uid)
		for data in datas:
			site = data[0]
			typee = data[1]
			province = data[2]
			city = data[3]
			url = data[4]
			number = data[5]
			title = data[6]
			name = data[7]
			date = data[8]
			start = data[9]
			end = data[10]
			crawled = data[11]
			content = data[12]
			us_id = data[13]
			company = data[14]
			es_insert(conn, site, typee, province, city, url, number, title, name, date, start, end, crawled, content, us_id, company)
		if uid == 31100000:
			break
		uid += 100000
		print uid 
	''' 
	#res = es.search(index="rccxtest", body={"query": { "term" : {"content": "电脑"}, "term" : {"content" : "期货大厦"}}})
	res = es.search(index="rccxtest", body={"query": { "terms" : {"content": ["期货","打印"],"minimum_should_match" : 2}}})
	ans_len = len(res['hits']['hits'])
	for i in range(ans_len):
		print res['hits']['hits'][i]['_source']['content']
	'''

