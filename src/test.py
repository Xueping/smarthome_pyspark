'''
Created on 6 Feb 2017

@author: xuepeng
'''

# from pyspark import SparkContext
# 
# logFile = "/home/xuepeng/spark-2.1.0-bin-hadoop2.7/README.md"
# sc = SparkContext("local", "Simple App")
# logData = sc.textFile(logFile).cache()
# numAs = logData.filter(lambda s: 'a' in s).count()
# numBs = logData.filter(lambda s: 'b' in s).count()
# print("Lines with a: %i, lines with b: %i" % (numAs, numBs))

line = '    '
line = line.strip()
print line
print len(line)



# import hashlib
# hash_object = hashlib.sha1('{1.1,2.0,1.1,2.0}')
# hex_dig = hash_object.hexdigest()
# print(hex_dig)

# tree = 'qwerea\n\nstree 0:'
# lines= tree.splitlines()
# data = []
# 
# squares = []
# 
# 
# print squares
# 
# del squares[:]
# 
# for i in range(100):
#     
#     if i % 10==5:
#         del squares[:]
#         print squares
#     squares.append(i)   
# 
# print squares

# for line in  lines: 
# #     if line.strip():
#         print line.strip()
# #         line = line.strip()
#     else : break
#     if not line : break