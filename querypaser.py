import os 
from elasticsearch import Elasticsearch, helpers

# stoptxt = open('query_desc.51-100.short.txt', "r")
# data2 = stoptxt.readline()
# # data_into_list = data2.replace('\n', ' ').split(".")
# # print(data)
# # print(len(data))
# # print(data_into_list)
# print(data2[6])
# stoptxt.close()
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



# data_into_list = data2.replace('\n', ' ').split(".")
# print(data)
# print(len(data))
# print(data_into_list)
# print(len(data2))
# stoptxt.close()

for i in querylist:
	query = i.lower()
	query = query.replace("document will",'')
	query = query.replace("document must",'')
	for j in data3:
		word = " "+j + " "
		query = query.replace(word,' ')
	print(query)

test = query.replace("91.    ", "")
print(test)
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


result = es.search(index="ap89_data2",
                   body={
                       "query": {
                           "function_score": {
                           		# "fields" :['_id'],
                           		# 'scroll': 1000,
                            	"query": {
                                   "match": {
                                       "content": "u.s. weapons systems"
                                   }
                               }
                           }
                       }
                   })


# print(result['hits']['hits'])
d=result['hits']['hits']
# d2 = [{'_index' : 'ap89_data2', '_type': '_doc', '_id': ' ap890130-0164 \n', '_score': 16.779451, '_source': d}]
# for i in d2:
# 	print(i['_id'])
result3 = [(item["_id"], item["_score"]) for item in d]
print(result3)


body2 = {
  "query": {
    "function_score": {
      "query": {
        "match": { "content": "u.s. weapons systems" }
      },
      "script_score": {
        "script": {
          "source": "Math.log(2 + doc['my-int'].value)"
        }
      }
    }
  }
}
# print(test[1])

# result = es.search(index="ap89_data2",body=body2)
