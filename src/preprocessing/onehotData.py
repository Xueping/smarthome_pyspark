# coding=utf-8

'''
Created on 4 May 2017

@author: xuepeng
'''

from datetime import datetime
import os
from pyspark import SparkContext
from pyspark.ml.feature import StringIndexer, OneHotEncoder
from pyspark.mllib.regression import LabeledPoint
from pyspark.sql.session import SparkSession
import codecs

import numpy as np


#生成中间文件（类似于数据库中格式），并存储到results.txt文件中
def processfile(record):
    data = []
    line_list = record[1].split('\n')
    file_No = record[0].split('.')[0].split('/')[-1]#modifyed by Xueping
    user=province=city = ""
    xulie = 1               #modifyed by Xueping
    data_record = []
    last_data_record = []   #modifyed by Xueping
    first_data_record = []  #modifyed by Xueping
    count = 0
    

    for item in line_list:#for each user information
        if item == '':
            break
        item_list = item.split(",")#change user information to list
        check = item_list[0]
        if check == "/":#one device content
            if item_list[2] == "poweron":
                if count == 0:#first one record in new sequence
                    first_data_record = [user,str(file_No)+"-"+str(xulie),str(1),item_list[8],"",item_list[3],item_list[4],\
                                         item_list[5],item_list[6],item_list[7],province,city] #modifyed by Xueping
                    xulie += 1 #modifyed by Xueping
                count += 1
                last_data_record = [user,str(file_No)+"-"+str(xulie),str(count),item_list[8],item_list[3],item_list[4],\
                                    item_list[5],item_list[6],item_list[7],province,city] #modifyed by Xueping
            else:#poweroff #modifyed by Xueping
                if len(first_data_record) > 0:
                    data_record = first_data_record
                    data_record[2] = last_data_record[2]
                    data_record[4] = last_data_record[3]
                    data.append(data_record)
                    last_data_record = []
                    first_data_record = []
                count = 0
           
        else:# another device id
            count = 0 #modifyed by Xueping
            user = check[3:]
            if item_list[1] == "None":
                province=city=""
                continue
            address = item_list[1]
            if address[2:4] == u"黑龙" or address[2:4] == u"内蒙":
                province = address[2:5]
                city = address[5:]
            else:
                province = address[2:4]
                city = address[4:]

    return data

#把列表转换为str类型
def map_list_string(record):
    for item in record:
        string = u','.join(item).encode('utf8')
    return string

#add number of sequence
def addNoOfSequenceToString(record):
    record[1].insert(0,record[0])
    string = u','.join(record[1]).encode('utf8')
    return string

def addNoToSequence(record):
    record[1].insert(0,record[0])
    return record[1]

#过滤序列长度小于2的项
def filterone(line):
    try:
        if int(line[2]) > 1: #modifyed by Xueping
            return True
        else:
            return False
    except Exception:
        print line
        
#filter invaild line
def filterline(line):
    for item in line[1]:
        if len(item) == 8:
            continue
        return True
    return False

#find the right weather information
def fine_weather(line,time):
    time_delta = 0
    index_f = 0
    haveWeatherInfo = False
    time_user = datetime.strptime(str(time),"%Y-%m-%d %H:%M")
    for index,item in enumerate(line):
        if len(item) == 8:
            haveWeatherInfo = True
            time_weather_str = str(item[1]+" "+item[2])
            time_delta_tmp = 0
            time_weather = datetime.strptime(time_weather_str,"%Y-%m-%d %H:%M:%S")
            if time_weather > time_user:
                time_delta_tmp = (time_weather-time_user).seconds
            else:
                time_delta_tmp = (time_user-time_weather).seconds
            if time_delta == 0 and index_f == 0:
                time_delta = time_delta_tmp
                index_f = index
            if time_delta_tmp < time_delta:
                index_f = index
                time_delta = time_delta_tmp
    if haveWeatherInfo :
        return line[index_f][1:]
    else:
        return ["","","","","","",""]

#add weather information
def mapone(line):
    line_list = []
    for item in line:
        if len(item) != 8:
            weather = fine_weather(line,item[4])
            item.extend(weather)
            line_list.append(item)
    return line_list

def dataPrepareStepOne(weatherList):
    
    sortList = sorted(weatherList,key = lambda item:item[2]) 
    rowList = []
    length = len(sortList)
    for x in range(0,length-3):
        row = []
        #    airTemp
        row.append(sortList[x][10])
        row.append(sortList[x+1][10])
        row.append(sortList[x+2][10])
        #air Speed
        row.append(sortList[x][6])
        row.append(sortList[x+1][6])
        row.append(sortList[x+2][6])
        #air direction
        row.append(sortList[x][7])
        row.append(sortList[x+1][7])
        row.append(sortList[x+2][7])
        
        #air mode
        row.append(sortList[x][8])
        row.append(sortList[x+1][8])
        row.append(sortList[x+2][8])
        
        #predict parameters
        row.append(sortList[x+3][15])
        row.append(sortList[x+3][6])
        row.append(sortList[x+3][7])
        row.append(sortList[x+3][8])
        row.append(sortList[x+3][10])
        rowList.append(row)
    return rowList

def filterTemp(item):
    if int(item[0]) >23 and int(item[0]) <29 and \
        int(item[1]) >23 and int(item[1]) <29  and \
        int(item[2]) >23 and int(item[2]) <29 and \
        int(item[-1]) >23 and int(item[-1]) <29 \
        and item[12]!= "":
        return True
    else :
        return False

speed = {"auto":0,"silence":1,"low":2,"mid":3,"high":4,"super":5}
direction = {"auto":0,"vdir":1,"hdir":2}
mode = {"wind":0,"cool":1,"heat":2,"auto":3,"dehu":4}

def transformer(item):
    item[0] = int(item[0])-24
    item[1] = int(item[1])-24
    item[2] = int(item[2])-24
    item[3] = speed[item[3]]
    item[4] = speed[item[4]]
    item[5] = speed[item[5]]
    item[6] = direction[item[6]]
    item[7] = direction[item[7]]
    item[8] = direction[item[8]]
    item[9] = mode[item[9]]
    item[10] = mode[item[10]]
    item[11] = mode[item[11]]
    item[12] = int(item[12])
    item[13] = speed[item[13]]
    item[14] = direction[item[14]]
    item[15] = mode[item[15]]
    item[16] = int(item[16])-24
    return item
    
def labelPoints(item):
        classLabel = item[-1]*90+ item[-2]*1+item[-3]*5+item[-4]*15 
#         classLabel = '{'+str(item[-1])+","+ str(item[-2]) +","+str(item[-3])+","+ str(item[-4])+'}' 
        return LabeledPoint(classLabel,item[0:13])
    
def temperatureLabelPoints(item):     
        return LabeledPoint(item[-1],item[0:13])
    
# INDEXING CATEGORICAL TEXT FEATURES FOR INPUT INTO LINEAR REGRESSION MODELS
def parseRowOneHotRegression(line):
    features = np.concatenate((np.array([line.temp1, line.temp2, line.temp3,
                                        line.speed1, line.speed2, line.speed3, line.weather]), 
                                        line.direction1Vec.toArray(), line.direction2Vec.toArray(), line.direction3Vec.toArray(),
                                        line.mode1Vec.toArray(), line.mode2Vec.toArray(), line.mode3Vec.toArray()), axis=0)
    labPt = LabeledPoint(line.temp, features)
    return  labPt


def parseRowOneHot(line):
    features = np.concatenate((         line.temp1Vec.toArray(), line.temp2Vec.toArray(), line.temp3Vec.toArray(),
                                        line.speed1Vec.toArray(), line.speed2Vec.toArray(), line.speed3Vec.toArray(),
                                        line.direction1Vec.toArray(), line.direction2Vec.toArray(), line.direction3Vec.toArray(),
                                        line.mode1Vec.toArray(), line.mode2Vec.toArray(), line.mode3Vec.toArray() ,line.weatherVec.toArray(),
                                        np.array([line.speed,line.direction,line.mode1,line.temp])), axis=0)
    return  features

def indexAndEncode(processedData,features):
    encodedFinal = processedData
    for feature in features:
        
        stringIndexer = StringIndexer(inputCol=feature, outputCol=feature+"Index")
        model = stringIndexer.fit(encodedFinal) # Input data-frame is the cleaned one from above
        indexed = model.transform(encodedFinal)
        encoder = OneHotEncoder(dropLast=False, inputCol=feature+"Index", outputCol=feature+"Vec")
        encodedFinal = encoder.transform(indexed)
    return encodedFinal
    
if __name__ == "__main__":
 
    data_file = "/home/xuepeng/data/smarthome/dec_data"
    weather_file = "/home/xuepeng/data/smarthome/weather/dec_weather.txt"
    
    sc = SparkContext("local[20]", "First_Spark_App")
    spark = SparkSession \
        .builder \
        .appName("First_Spark_App") \
        .getOrCreate()
    #original Data
    raw_data = sc.wholeTextFiles(data_file)
    #weather information
    weather_data = sc.textFile(weather_file).map(lambda line: line.split(","))\
                     .map(lambda line:(line[0]+" "+line[1],line)).cache()

    finalDataWithoutWeather = raw_data.flatMap(processfile).\
                    filter(filterone).map(lambda line:(line[0],line)).\
                    groupByKey().mapValues(list).filter(lambda item:len(item[1]) > 3).\
                    map(lambda item: (str(len(item[1])),item[1])).\
                    flatMapValues(lambda item:item).map(addNoToSequence)
                                        
    finalDataWithWeather = finalDataWithoutWeather.map(lambda line:(line[-1]+" "+line[4][0:10],line)).union(weather_data)\
                     .groupByKey().mapValues(list).filter(filterline).map(lambda line:line[1])\
                     .flatMap(mapone).map(lambda line:(line[1]+" "+line[4],line)).sortByKey().map(lambda line:line[1])\
    
    #Extract features
    features = finalDataWithWeather.map(lambda item :(item[1],item))\
                    .groupByKey().mapValues(list).map(lambda item:item[1])\
                    .flatMap(dataPrepareStepOne)
                    
    transformedData = features.filter(filterTemp).map(transformer)
    
    #Convert RDD Data to DataFrame
    processedData = transformedData.\
        toDF(['temp1', 'temp2','temp3','speed1','speed2','speed3','direction1','direction2','direction3',\
              'mode1','mode2','mode3','weather','speed','direction','mode','temp'])
    
    #Indicate which Feature should be one-hot encoding  
    categoricalFeatures = ['temp1', 'temp2','temp3','speed1','speed2','speed3','direction1','direction2',\
                           'direction3','mode1','mode2','mode3','weather']
    
    #Get one-hot encoding features which include last four labels
    onehotFeatures = indexAndEncode(processedData,categoricalFeatures).rdd.map(parseRowOneHot)
    
    #Write one-hot encoding features in file or do next step for processing data
    if os.path.exists("oneHostDataset.txt"):
            os.remove("oneHostDataset.txt")
    finalDS =  codecs.open('oneHostDataset.txt',"a+","utf-8")
    ds = onehotFeatures.map(lambda item :  ','.join('{:.0f}'.format(x) for x in item)).collect()
    finalDS.write('\n'.join(ds))
    finalDS.close()

    sc.stop()