import os 


# stoptxt = open('query_desc.51-100.short.txt', "r")
# data2 = stoptxt.readline()
# # data_into_list = data2.replace('\n', ' ').split(".")
# # print(data)
# # print(len(data))
# # print(data_into_list)
# print(data2[6])
# stoptxt.close()

querylist = []
with open('query_desc.51-100.short.txt') as file:
    while (line := file.readline().rstrip()):
        querylist.append(line)

test =  querylist[0].split("   ")


print(test[1])