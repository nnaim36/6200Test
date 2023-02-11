import os 
from os.path import exists
from elasticsearch import Elasticsearch, helpers
from collections import Counter
from nltk.tokenize import word_tokenize
from nltk.stem import PorterStemmer
import pandas as pd
import math

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
file1 = open("results_file.txt","w")

file2 = open("results_file2.txt","w")
file3 = open("results_file3.txt", "w")
file4 = open("results_file4.txt", "w")
file5 = open("results_file5.txt", "w")
file6 = open("results_file6.txt", "w")

def okapitf(df,termlist):
	scorelist =[]
	scorerow = []
	averagecorp = df.loc[:,"totnum"].mean()
	for i in range(len(df)):
		# tfreq = df.iloc[i,"term1"]
		doclength = df.loc[i,"totnum"]
		tscore =0
		for j in range(1,len(termlist)-1):
			tname = 'term'+str(j)
			tfreq= df.loc[i,tname]
			bottom1 = tfreq+.5
			bottom2 = doclength/averagecorp
			bottom2 = bottom2*1.5
			bottomt = bottom1 +bottom2
			score = tfreq/bottomt
			tscore = tscore+score
		scorelist.append(tscore)
		# if len(termlist) ==3:
	print(scorelist)
	return scorelist

def tf_idf(df, termlist):
	scorelist =[]
	scorerow = []
	averagecorp = df.loc[:,"totnum"].mean()
	for i in range(len(df)):
		# tfreq = df.iloc[i,"term1"]
		doclength = df.loc[i,"totnum"]
		tscore =0
		for j in range(1,len(termlist)-1):
			tname = 'term'+str(j)
			tfreq= df.loc[i,tname]
			bottom1 = tfreq+.5
			bottom2 = doclength/averagecorp
			bottom2 = bottom2*1.5
			bottomt = bottom1 +bottom2
			score = tfreq/bottomt
			l = 84648 / len(df)
			score = score*math.log10(l)
			
			tscore = tscore+score
		scorelist.append(tscore)
		# if len(termlist) ==3:
	print(scorelist)
	return scorelist

def bm25(df, termlist):



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
	# print(result3)
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
	# print(result3)
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
	q = q.replace(" worldwide"," ")
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

	q=q.replace("supporters ","")
	q=q.replace(" assets"," ")

	q=q.replace(" agreement "," ")
	q=q.replace(" commercial "," ")

	q=q.replace("non ","")
	q=q.replace(" high "," ")
	q=q.replace(" use "," ")
	q=q.replace(" dual "," ")
	q=q.replace(" states "," ")
	q=q.replace(" transfer "," ")
	q=q.replace(" goods "," ")

	q=q.replace(" products"," ")
	q=q.replace("concerns ","")
	q=q.replace(" fine"," ")
	q=q.replace(" diameter"," ")
	q=q.replace("studies ","")

	q=q.replace("the ","")
	q=q.replace(" use","")
	q=q.replace(" pay "," ")




	q=q.replace("  "," ")
	q=q.replace("  "," ")
	q = removedup(q)
	q=q.replace("  "," ")

	print(q)

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
qnlist = []
dflist=[]
statslist = []
count2 =0
count =1
# count3 = 0
print(querylist)
print(len(querylist))
for i in querylist:
	print(count)
	query = i.lower()
	# print(query)
	# query2 = query.split(".   ")
	if ".    " in query:
		query2 = query.split(".    ")
	else:
		query2 = query.split(".   ")
	qn = query2[0]
	qs = query2[1]
	for j in data3:
		# if count2 !=3 and count2 != 2:
		word = " "+j + " "
		qs = qs.replace(word,' ')
	qnlist.append(qn)
		
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
				file1.write(qn+" Q0 "+idq +" "+str(count)+" "+str(rating)+" Exp\n")
				# writeout(file2,qn,idq,count,rating)
				# print(idq)
				# print(k[2]["text"])
		# dflist.append()

	if count2 ==0:
		q2_result = queryrun2(str(qs))
		df1 = pd.DataFrame(columns = ['ID','totnum','term1','term2','term3', 'term4'])
		if q2_result is not None:
			for k in q2_result:
				idq = k[0].replace("\n",'')
				rating = k[1]
				stats = getstats(k[2]["text"],str(qs))
				submitline = (id)
				df1.loc[len(df1)] = (idq, stats[0], stats[1], stats[2],stats[3],stats[4])
			dflist.append(df1)
			statslist.append(stats)
				# writeout(file2,qn,idq,count,rating)
			# print(df1)

	if count2 == 1:
		q2_result = queryrun2(str(qs))
		df2 = pd.DataFrame(columns = ['ID','totnum','term1','term2','term3', 'term4'])
		if q2_result is not None:
			for k in q2_result:
				idq = k[0].replace("\n",'')
				stats = getstats(k[2]["text"],str(qs))
				stats = getstats(k[2]["text"],str(qs))
				submitline = (id)
				print("thee lenght of starts: " +str(len(stats)))
				df2.loc[len(df2)] = (idq, stats[0], stats[1], stats[2],stats[3],stats[4])
			dflist.append(df2)
			statslist.append(stats)
			okscore = okapitf(df2,stats)
			df2["score"] =okscore
			print(df2)

				
	if count2 == 2:
		q2_result = queryrun2(str(qs))
		df3 = pd.DataFrame(columns = ['ID','totnum','term1','term2','term3', 'term4'])
		if q2_result is not None:
			for k in q2_result:
				idq = k[0].replace("\n",'')
				stats = getstats(k[2]["text"],str(qs))
				stats = getstats(k[2]["text"],str(qs))
				submitline = (id)
				df3.loc[len(df3)] = (idq, stats[0], stats[1], stats[2],stats[3],stats[4])
			dflist.append(df3)
			statslist.append(stats)
				
	if count2 == 3:
		q2_result = queryrun2(str(qs))
		df4 = pd.DataFrame(columns = ['ID','totnum','term1','term2','term3', 'term4'])
		if q2_result is not None:
			for k in q2_result:
				idq = k[0].replace("\n",'')
				stats = getstats(k[2]["text"],str(qs))
				stats = getstats(k[2]["text"],str(qs))
				submitline = (id)
				df4.loc[len(df4)] = (idq, stats[0], stats[1], stats[2],stats[3],stats[4]) 
			dflist.append(df4)      

	if count2 == 4:
		q2_result = queryrun2(str(qs))
		df5 = pd.DataFrame(columns = ['ID','totnum','term1','term2','term3', 'term4'])
		if q2_result is not None:
			for k in q2_result:
				idq = k[0].replace("\n",'')
				stats = getstats(k[2]["text"],str(qs))
				stats = getstats(k[2]["text"],str(qs))
				submitline = (id)
				df5.loc[len(df5)] = (idq, stats[0], stats[1], stats[2],stats[3],stats[4])
			dflist.append(df5)
			statslist.append(stats)
	
	if count2 == 5:
		q2_result = queryrun2(str(qs))
		df6 = pd.DataFrame(columns = ['ID','totnum','term1','term2','term3', 'term4'])
		if q2_result is not None:
			for k in q2_result:
				idq = k[0].replace("\n",'')
				stats = getstats(k[2]["text"],str(qs))
				stats = getstats(k[2]["text"],str(qs))
				submitline = (id)
				df6.loc[len(df6)] = (idq, stats[0], stats[1], stats[2],stats[3],stats[4])
			dflist.append(df6)
			statslist.append(stats)
				
	if count2 == 6:
		q2_result = queryrun2(str(qs))
		df7 = pd.DataFrame(columns = ['ID','totnum','term1','term2','term3', 'term4'])
		if q2_result is not None:
			for k in q2_result:
				idq = k[0].replace("\n",'')
				stats = getstats(k[2]["text"],str(qs))
				stats = getstats(k[2]["text"],str(qs))
				submitline = (id)
				df7.loc[len(df7)] = (idq, stats[0], stats[1], stats[2],stats[3],stats[4])
			dflist.append(df7)
			statslist.append(stats)
				
	if count2 == 7:
		q2_result = queryrun2(str(qs))
		df8 = pd.DataFrame(columns = ['ID','totnum','term1','term2','term3', 'term4'])
		if q2_result is not None:
			for k in q2_result:
				idq = k[0].replace("\n",'')
				stats = getstats(k[2]["text"],str(qs))
				stats = getstats(k[2]["text"],str(qs))
				submitline = (id)
				df8.loc[len(df8)] = (idq, stats[0], stats[1], stats[2],stats[3],stats[4])
			dflist.append(df8)        
				
				
	if count2 == 8:
		q2_result = queryrun2(str(qs))
		df9 = pd.DataFrame(columns = ['ID','totnum','term1','term2','term3'])
		if q2_result is not None:
			for k in q2_result:
				idq = k[0].replace("\n",'')
				stats = getstats(k[2]["text"],str(qs))
				stats = getstats(k[2]["text"],str(qs))
				submitline = (id)
				df9.loc[len(df9)] = (idq, stats[0], stats[1], stats[2],stats[3])
			dflist.append(df9)
			statslist.append(stats)
				
	if count2 == 9:
		q2_result = queryrun2(str(qs))
		df10 = pd.DataFrame(columns = ['ID','totnum','term1','term2','term3'])
		if q2_result is not None:
			for k in q2_result:
				idq = k[0].replace("\n",'')
				stats = getstats(k[2]["text"],str(qs))
				stats = getstats(k[2]["text"],str(qs))
				submitline = (id)
				df10.loc[len(df10)] = (idq, stats[0], stats[1], stats[2],stats[3])
			dflist.append(df10)
			statslist.append(stats)
				
	if count2 == 10:
		q2_result = queryrun2(str(qs))
		df11 = pd.DataFrame(columns = ['ID','totnum','term1','term2','term3', 'term4'])
		if q2_result is not None:
			for k in q2_result:
				idq = k[0].replace("\n",'')
				stats = getstats(k[2]["text"],str(qs))
				stats = getstats(k[2]["text"],str(qs))
				submitline = (id)
				df11.loc[len(df11)] = (idq, stats[0], stats[1], stats[2],stats[3],stats[4])
			dflist.append(df11)
			statslist.append(stats)
				
	if count2 == 11:
		q2_result = queryrun2(str(qs))
		df12 = pd.DataFrame(columns = ['ID','totnum','term1','term2','term3', 'term4'])
		if q2_result is not None:
			for k in q2_result:
				idq = k[0].replace("\n",'')
				stats = getstats(k[2]["text"],str(qs))
				stats = getstats(k[2]["text"],str(qs))
				submitline = (id)
				df12.loc[len(df12)] = (idq, stats[0], stats[1], stats[2],stats[3],stats[4])
			dflist.append(df12)
			statslist.append(stats)
				
	if count2 == 12:
		q2_result = queryrun2(str(qs))
		df13 = pd.DataFrame(columns = ['ID','totnum','term1','term2','term3'])
		if q2_result is not None:
			for k in q2_result:
				idq = k[0].replace("\n",'')
				stats = getstats(k[2]["text"],str(qs))
				stats = getstats(k[2]["text"],str(qs))
				submitline = (id)
				df13.loc[len(df13)] = (idq, stats[0], stats[1], stats[2],stats[3])
			dflist.append(df13)
			statslist.append(stats)     
				
	if count2 == 13:
		q2_result = queryrun2(str(qs))
		df14 = pd.DataFrame(columns = ['ID','totnum','term1','term2','term3', 'term4'])
		if q2_result is not None:
			for k in q2_result:
				idq = k[0].replace("\n",'')
				stats = getstats(k[2]["text"],str(qs))
				stats = getstats(k[2]["text"],str(qs))
				submitline = (id)
				df14.loc[len(df14)] = (idq, stats[0], stats[1], stats[2],stats[3],stats[4])
			dflist.append(df14)
			statslist.append(stats)
				
	if count2 == 14:
		q2_result = queryrun2(str(qs))
		df15 = pd.DataFrame(columns = ['ID','totnum','term1','term2','term3', 'term4'])
		if q2_result is not None:
			for k in q2_result:
				idq = k[0].replace("\n",'')
				stats = getstats(k[2]["text"],str(qs))
				stats = getstats(k[2]["text"],str(qs))
				submitline = (id)
				df15.loc[len(df15)] = (idq, stats[0], stats[1], stats[2],stats[3],stats[4])
			dflist.append(df15)
			statslist.append(stats)


	if count2 == 15:
		q2_result = queryrun2(str(qs))
		df16 = pd.DataFrame(columns = ['ID','totnum','term1','term2','term3', 'term4'])
		if q2_result is not None:
			for k in q2_result:
				idq = k[0].replace("\n",'')
				stats = getstats(k[2]["text"],str(qs))
				stats = getstats(k[2]["text"],str(qs))
				submitline = (id)
				df16.loc[len(df16)] = (idq, stats[0], stats[1], stats[2],stats[3],stats[4]) 
			dflist.append(df16)
			statslist.append(stats)

				
	if count2 == 16:
		q2_result = queryrun2(str(qs))
		df17 = pd.DataFrame(columns = ['ID','totnum','term1','term2','term3', 'term4'])
		if q2_result is not None:
			for k in q2_result:
				idq = k[0].replace("\n",'')
				stats = getstats(k[2]["text"],str(qs))
				stats = getstats(k[2]["text"],str(qs))
				submitline = (id)
				df17.loc[len(df17)] = (idq, stats[0], stats[1], stats[2],stats[3],stats[4])  
			dflist.append(df17)
			statslist.append(stats)  


	if count2 == 17:
		q2_result = queryrun2(str(qs))
		df18 = pd.DataFrame(columns = ['ID','totnum','term1','term2','term3', 'term4'])
		if q2_result is not None:
			for k in q2_result:
				idq = k[0].replace("\n",'')
				stats = getstats(k[2]["text"],str(qs))
				stats = getstats(k[2]["text"],str(qs))
				submitline = (id)
				df18.loc[len(df18)] = (idq, stats[0], stats[1], stats[2],stats[3],stats[4])
			dflist.append(df18)
			statslist.append(stats)


	if count2 == 18:
		q2_result = queryrun2(str(qs))
		df19 = pd.DataFrame(columns = ['ID','totnum','term1','term2','term3', 'term4'])
		if q2_result is not None:
			for k in q2_result:
				idq = k[0].replace("\n",'')
				stats = getstats(k[2]["text"],str(qs))
				stats = getstats(k[2]["text"],str(qs))
				submitline = (id)
				df19.loc[len(df19)] = (idq, stats[0], stats[1], stats[2],stats[3],stats[4])
			dflist.append(df19)
			statslist.append(stats)


	if count2 == 19:
		q2_result = queryrun2(str(qs))
		df20 = pd.DataFrame(columns = ['ID','totnum','term1','term2','term3'])
		if q2_result is not None:
			for k in q2_result:
				idq = k[0].replace("\n",'')
				stats = getstats(k[2]["text"],str(qs))
				stats = getstats(k[2]["text"],str(qs))
				submitline = (id)
				df20.loc[len(df20)] = (idq, stats[0], stats[1], stats[2],stats[3]) 
			dflist.append(df20)
			statslist.append(stats)

	if count2 == 20:
		q2_result = queryrun2(str(qs))
		df21 = pd.DataFrame(columns = ['ID','totnum','term1','term2','term3'])
		if q2_result is not None:
			for k in q2_result:
				idq = k[0].replace("\n",'')
				stats = getstats(k[2]["text"],str(qs))
				stats = getstats(k[2]["text"],str(qs))
				submitline = (id)
				df21.loc[len(df21)] = (idq, stats[0], stats[1], stats[2],stats[3])
			dflist.append(df21)
			statslist.append(stats)

	if count2 == 21:
		q2_result = queryrun2(str(qs))
		df22 = pd.DataFrame(columns = ['ID','totnum','term1','term2','term3', 'term4'])
		if q2_result is not None:
			for k in q2_result:
				idq = k[0].replace("\n",'')
				stats = getstats(k[2]["text"],str(qs))
				stats = getstats(k[2]["text"],str(qs))
				submitline = (id)
				df22.loc[len(df22)] = (idq, stats[0], stats[1], stats[2],stats[3],stats[4])
			dflist.append(df22)
			statslist.append(stats)

	if count2 == 22:
		q2_result = queryrun2(str(qs))
		df23 = pd.DataFrame(columns = ['ID','totnum','term1','term2','term3'])
		if q2_result is not None:
			for k in q2_result:
				idq = k[0].replace("\n",'')
				stats = getstats(k[2]["text"],str(qs))
				stats = getstats(k[2]["text"],str(qs))
				submitline = (id)
				df23.loc[len(df23)] = (idq, stats[0], stats[1], stats[2],stats[3]) 
			dflist.append(df23)
			statslist.append(stats)

	if count2 == 23:
		q2_result = queryrun2(str(qs))
		df24 = pd.DataFrame(columns = ['ID','totnum','term1','term2','term3'])
		if q2_result is not None:
			for k in q2_result:
				idq = k[0].replace("\n",'')
				stats = getstats(k[2]["text"],str(qs))
				stats = getstats(k[2]["text"],str(qs))
				submitline = (id)
				df24.loc[len(df24)] = (idq, stats[0], stats[1], stats[2],stats[3]) 
			dflist.append(df24)
			statslist.append(stats)

	if count2 == 24:
		q2_result = queryrun2(str(qs))
		df25 = pd.DataFrame(columns = ['ID','totnum','term1','term2','term3'])
		if q2_result is not None:
			for k in q2_result:
				idq = k[0].replace("\n",'')
				stats = getstats(k[2]["text"],str(qs))
				stats = getstats(k[2]["text"],str(qs))
				submitline = (id)
				df25.loc[len(df25)] = (idq, stats[0], stats[1], stats[2],stats[3]) 
			dflist.append(df25)
			statslist.append(stats)


	count2 = count2 +1
	count = count +1


for i in range(0,len(dflist)):

	if(query2model == True):
		if exists("./results_file2.txt") == True:
			os.remove("./results_file2.txt")
		file = open("results_file2.txt","w")
		okayscore = okapitf(dflist[i],statslist[1])

	dflist[i]["score"] = okayscore
	dflist[i].sort_values(by="score", ascending = False, inplace=True)
	for j in range(len(dflist[i])):
		val= dflist[i].loc[j,"score"]
	# file2.write("hello\n")



# test = query.replace("91.    ", "")
# print(test)
es = Elasticsearch("http://localhost:9200")

print("*****************************************")

file1.close()
