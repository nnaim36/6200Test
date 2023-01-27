# from elasticsearch import Elasticsearch, helpers
import os
from nltk.tokenize import word_tokenize
from nltk.stem import PorterStemmer
import re

x =5
docidlist = []
textlist = []
# client = Elasticsearch("http://localhost:9200")
count = 0
path =  "ap89_collection"
count = 0 
textcheck = False
textvar = ''
for i in os.listdir(path):  
    # print(i)
    data = []
    file = "ap89_collection\\"+i
    for line in open(file,encoding = "utf8", errors = 'ignore'):
        # data += [str(line)]
        line2 = str(line).lower()

    # print(data)

        if i == "ap890101":
            if '<doc>' in line2:
                count =count+1
            if '<docno>' in line2:
                docid = line2.replace("<docno>",'')
                docid = docid.replace("</docno>",'')
                docidlist.append(docid)
                print(docid)
            if count ==1:
                lower = str(line).lower()
                lower1 = lower.split('<doc>')
                print(lower1[0])

            # print(count)

            if textcheck == True:
                textvar += line2
            if '<text>' in line2:
                textcheck = True
                textvar += line2
            if '</text>' in line2:
                textvar = textvar.replace("<text>",'')
                textvar = textvar.replace("</text>",'')
                textlist.append(textvar)
                textcheck = False
                print(textvar)
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
            #   print(lower)

print(len(docidlist))
print(len(textlist))
print(count)
# upload = helpers.bulk(client,data)
# print(len(data))
# ELASTIC_PASSWORD = "<password>"









mappings = {
    "mappings": {
        "properties": {
            "ID": {
                "type": "text","analyzer": "standard" # formerly "string"
            },
            "TEXT": {
                "type": "text","analyzer": "standard"
            }
        }
    }
}

client.indices.create(index='ap files', mapping = mappings)
