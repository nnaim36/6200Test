from elasticsearch import Elasticsearch, helpers
import os
from nltk.tokenize import word_tokenize
from nltk.stem import PorterStemmer
import re
import pandas as pd


ps = PorterStemmer()
x =5
docidlist = []
textlist = []
es = Elasticsearch("http://localhost:9200")
print(es)
count = 0
path =  "ap89_collection"
count = 0 
textcheck = False
textvar = ''
gopar = True

stoptxt = open('my_stoplist.txt', "r")
data2 = stoptxt.read()
# data_into_list = data2.replace('\n', ' ').split(".")
# print(data)
# print(len(data))
# print(data_into_list)
print(len(data2))
stoptxt.close()

for i in os.listdir(path):	
	# print(i)
	data = []
	file = "ap89_collection\\"+i
	for line in open(file,encoding = "utf8", errors = 'ignore'):
		# data += [str(line)]
		line2 = str(line).lower()
		newsent = ''
	# print(data)

		# if i == "ap890101":
		if gopar == True:	
			if '<doc>' in line2:
				count =count+1
			if '<docno>' in line2:
				docid = line2.replace("<docno>",'')
				docid = docid.replace("</docno>",'')
				docidlist.append(docid)
				# print(docid)
			if count ==1:
				lower = str(line).lower()
				lower1 = lower.split('<doc>')
				# print(lower1[0])

			# print(count)

			if textcheck == True:
				textvar += line2
			if '<text>' in line2:
				textcheck = True
				textvar += line2
			if '</text>' in line2:
			# 	textvar = textvar.replace("<text>",'')
			# 	textvar = textvar.replace("</text>",'')
			# 	textlist.append(textvar)
				textcheck = False
			# 	print(textvar)
			# 	textvar = ''

			if '</doc>' in line2:
				textvar = textvar.replace("<text>",'')
				textvar = textvar.replace("</text>",'')
				words = word_tokenize(textvar)
				newsent = ''
				for w in words:
					anew = ps.stem(w)
					newsent = newsent +" "+anew

				newsent = newsent.replace(",",'')
				newsent = newsent.replace("!",'')
				newsent = newsent.replace(".",'')
				newsent = newsent.replace("?",'')
				newsent = newsent.replace("{",'')
				newsent = newsent.replace("}",'')
				newsent = newsent.replace("_",'')
				newsent = newsent.replace(";",'')
				newsent = newsent.replace(":",'')
				newsent = newsent.replace("  ",' ')
				textlist.append(newsent)
				# print(newsent)
				textcheck = False
				# print(newsent)
				textvar = ''


			lower=str(line).lower()

			lower2 = lower.split("<doc>")
			test = re.findall(r"<doc>", lower2[0])
			# print(test)
			if '<doc>' in lower and count ==0:
				p=7
				# print(lower)
				# count =count +1
				# print("heres doc")
			# else:
			# 	print(lower)

print(len(docidlist))
print(len(textlist))
print(count)
# upload = helpers.bulk(client,data)
# print(len(data))
# ELASTIC_PASSWORD = "<password>"

df = pd.DataFrame(
	{'DOC_ID':docidlist,
	'text' : textlist

	})


# mappings = {
#     "mappings": {
#         "properties": {
#             "ID": {
#                 "type": "text","analyzer": "standard" # formerly "string"
#             },
#             "TEXT": {
#                 "type": "text","analyzer": "standard"
#             }
#         }
#     }
# }


# client.indices.create(index="movies", mappings=mappings)
# client.indices.create(index='ap_files', mappings = mappings)

#****************************************************************
body = {
    "settings" : {
        "number_of_shards": 1,
        "number_of_replicas": 1,
        "analysis": {
            "filter": {
                "english_stop": {
                    "type": "stop",
                    "stopwords": data2
                }
            },
            "analyzer": {
                "stopped": {
                    "type": "custom",
                    "tokenizer": "standard",
                    "filter": [
                        "lowercase",
                        "english_stop"
                    ]
                }
            }
      }
    },
    "mappings": {
        "properties": {
            "content": {
                "type": "text",
                "fielddata": True,
                "analyzer": "stopped",
                "index_options": "positions"
            }
        }
    }
}

es.indices.create(index="ap89_data", body=body)
# es.indices.create(index="apfiletest1", body=body)

for i, row in df.iterrows():
	doc = {
		"content": row["text"]
	}

# print(df)

	# print(df['DOC_ID'].iloc[i])
	# es.index(index ="apfiletest1", id = df['DOC_ID'].iloc[i],document =doc)
	es.index(index ="ap89_data", id = str(df['DOC_ID'].iloc[i]),document =doc)

#**********************************************************************
