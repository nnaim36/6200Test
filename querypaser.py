import os 
from os.path import exists
from elasticsearch import Elasticsearch, helpers
from collections import Counter
from nltk.tokenize import word_tokenize
from nltk.stem import PorterStemmer

# stoptxt = open('query_desc.51-100.short.txt', "r")
# data2 = stoptxt.readline()
# # data_into_list = data2.replace('\n', ' ').split(".")
# # print(data)
# # print(len(data))
# # print(data_into_list)
# print(data2[6])
# stoptxt.close()

ps = PorterStemmer()
stoplist= [" a "," identify "," or "," even "," describe "," to "," the "," of ", " by ", " has ", " and "]
querylist = []
data3 = []
with open('query_desc.51-100.short.txt') as file:
    while (line := file.readline().rstrip()):
        querylist.append(line)

test =  querylist[0].split("   ")

with open('my_stoplist.txt') as file:
	while (line := file.readline().rstrip()):
		data3.append(line)

if exists("./results_file.txt") == True:
	os.remove("./results_file.txt")
	print("we are doing it")

# with open("results_file.txt","w") as file2:
# 	print(" ")
file2 = open("results_file.txt","w")


# data_into_list = data2.replace('\n', ' ').split(".")
# print(data)
# print(len(data))
# print(data_into_list)
# print(len(data2))
# stoptxt.close()

def queryrun(q):
	es = Elasticsearch("http://localhost:9200")
	result = es.search(index="ap89_data13test",
	                   body={
	                       "query": {
	                           "function_score": {
	                           		# "fields" :['_id'],
	                           		# 'scroll': 1000,
	                            	"query": {
	                                   "match": {
	                                       "text": q
	                                   }
	                               }
	                           }
	                       },"size": 1000
	                   })


	# result2 = es.search(index="ap89_data2",body={"size": 1000, "query": { "match": { "text": " text to search" } }})

	# print(result['hits']['hits'])
	d=result['hits']['hits']
	# print(d)
	# d2 = [{'_index' : 'ap89_data2', '_type': '_doc', '_id': ' ap890130-0164 \n', '_score': 16.779451, '_source': d}]
	# for i in d2:
	# 	print(i['_id'])
	result3 = [(item["_id"], item["_score"],item["_source"]) for item in d]
	# file2.close()
	print(result3)
	return result3


def writeout(file,qnum,iddoc,rank,score):
	# path= './'+name+'.txt'
	# if exists(path) == True:
	# 	os.remove("./results_file.txt")
	# 	print("we are doing it")
	# file2 = open(path,"w")
	# # count=1
	file.write(qnum+" Q0 "+iddoc +" "+str(rank)+" "+str(score)+" Exp")
	

def cleanquery(q,data3):

	q = q.replace("document ",'')
	# q = q.replace("document",'')
	q = q.replace("u.s.", "usa")
	q = q.replace(".","")
	q= q.replace(" the "," ")
	q= q.replace(", "," ")
	q= q.replace("(","")
	q= q.replace(")","")
	q= q.replace("-"," ")
	q= q.replace('"', '')
	# q= q.replace("' "," ")

	# qlist = 
	# for i in data3:
	# 	for j in 

	#85
	q = q.replace("against "," ")
	q = q.replace("worldwide "," ")
	#71
	q= q.replace("second "," ")
	q= q.replace("country "," ")
	q= q.replace(" country"," ")
	q= q.replace("land "," ")
	q= q.replace("air "," ")
	q= q.replace("area "," ")
	q= q.replace("border "," ")
	#64 good
	#62
	q= q.replace("attempted "," ")

	q=q.replace("  "," ")
	q=q.replace("  "," ")
	q = removedup(q)
	q=q.replace("  "," ")

	# print(q)

	ps = PorterStemmer()
	newq = ''
	words = word_tokenize(q)
	for w in words:
		# print(w)
		anew = ps.stem(w)
		newq = newq+" "+anew
	q = newq

	return q

def removedup(q):
	words = q.split(" ")
	words = Counter(words)
	unwords = " ".join(words.keys())
	return unwords

count2 =0
count =1
for i in querylist:
	query = i.lower()
	# query2 = query.split(".   ")
	if ".    " in query:
		query2 = query.split(".    ")
	else:
		query2 = query.split(".   ")
	qn = query2[0]
	qs = query2[1]
	# query = query.replace("document will",'')
	# query = query.replace("document must",'')
	# query = query.replace()
	for j in data3:
		word = " "+j + " "
		qs = qs.replace(word,' ')
	qs = cleanquery(qs,data3)
	# qn = qn.replace("'\n'","")

	# print(qs)
	# print(qn)

	if count2 ==0 :
		result5 = queryrun(str(qs))
		if result5 is not None:
			for k in result5:
				# print("************")
				# writeout()
				idq = k[0].replace("\n",'')
				rating = k[1]
				file2.write(qn+" Q0 "+idq +" "+str(count)+" "+str(rating)+" Exp\n")
				# writeout(file2,qn,idq,count,rating)
				print(idq)
				print(k[2]["text"])


	count2 = count2 +1
	count = count +1
	# file2.write("hello\n")



# test = query.replace("91.    ", "")
# print(test)
es = Elasticsearch("http://localhost:9200")
# q = es.search(
# 	index = "ap89_data",
# 	body= {
# 	"size" : 1000,
# 	"query" : {
# 		"function_score": {
# 			"content" : test
# 		}
# 	}
# 	}
# 	)

# def queryrun(q):
# 	result = es.search(index="ap89_data13test",
# 	                   body={
# 	                       "query": {
# 	                           "function_score": {
# 	                           		# "fields" :['_id'],
# 	                           		# 'scroll': 1000,
# 	                            	"query": {
# 	                                   "match": {
# 	                                       "text": q
# 	                                   }
# 	                               }
# 	                           }
# 	                       },"size": 1000
# 	                   })


# 	# result2 = es.search(index="ap89_data2",body={"size": 1000, "query": { "match": { "text": " text to search" } }})

# 	# print(result['hits']['hits'])
# 	d=result['hits']['hits']
# 	# d2 = [{'_index' : 'ap89_data2', '_type': '_doc', '_id': ' ap890130-0164 \n', '_score': 16.779451, '_source': d}]
# 	# for i in d2:
# 	# 	print(i['_id'])
# 	result3 = [(item["_id"], item["_score"]) for item in d]
# 	print(result3)


# body2 = {
#   "query": {
#     "function_score": {
#       "query": {
#         "match": { "content": "u.s. weapons system" }
#       },
#       "script_score": {
#         "script": {
#           "source": "Math.log(2 + doc['my-int'].value)"
#         }
#       }
#     }
#   }
# }
# print(test[1])

# result = es.search(index="ap89_data2",body=body2)
print("*****************************************")
# ans = es.termvectors(index="ap89_data2",
#                         id="ap890101-0014",
#                         fields = "content")

# print(ans)
# ans = es.termvectors(index="ap89_data13test",
#                         id="ap890101-0009",
#                         body={
#                             # "fields": ["text","system"],
#                             "fields": ["text"],
#                             "term_statistics": True,
#                             "field_statistics": True
#                             # "doc" : {"plot":"system"}
#                         })["term_vectors"]

# ans2 = es.termvectors(index="ap89_data2",
#                         id='ap890130-0164',
#                         fields=["content"]
#                         )


# field_term_vectors = ans2['system']['content']
# print(field_term_vectors)
# ["doc"]:{"plot":"system"}
# print(ans)

file2.close()
