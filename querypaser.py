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


	d=result['hits']['hits']
	result3 = [(item["_id"], item["_score"],item["_source"]) for item in d]
	# file2.close()
	print(result3)
	return result3

def queryrun2(q):
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
	                       }
	                   })


	d=result['hits']['hits']
	result3 = [(item["_id"], item["_score"],item["_source"]) for item in d]
	# file2.close()
	print(result3)
	return result3

def writeout(file,qnum,iddoc,rank,score):
	file.write(qnum+" Q0 "+iddoc +" "+str(rank)+" "+str(score)+" Exp")

def getstats(text,query):
	testlist = text.split(" ")
	querylist = query.split(" ")
    resultlist=[]
	size = len(testlist)
    resultlist.append(size)
	matchnum =0
	for i in querylist:
		matchnum =0
		for j in testlist:
			if i == j:
				matchnum = matchnum +1

		resultlist.append(matchnum)
        
    return resultlist
	

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


	if count2 ==0:
		q2_result = queryrun2(str(qs))
        	df1 = pd.DataFrame(columns = ['ID','totnum','term1','term2','term3', 'ter,4'])
		if q2_result is not None:
			for k in q2_result:
				idq = k[0].replace("\n",'')
				rating = k[1]
                		stats = getstats(k[2]["text"],str(gs))
                		submitline = (id)
                		df1.loc[len(df1)] = (idq, stats[0], stats[1], stats[])
				# writeout(file2,qn,idq,count,rating)
				print(idq)
				print(k[2]["text"])

	count2 = count2 +1
	count = count +1
	# file2.write("hello\n")



# test = query.replace("91.    ", "")
# print(test)
es = Elasticsearch("http://localhost:9200")

print("*****************************************")

file2.close()
