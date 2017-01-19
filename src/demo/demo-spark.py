# -*- coding: UTF-8 -*-
#原始数据文件放在当前目录下的csv目录下，数据清洗最终结果会放在当前目录下的output目录下
#程序运行方式（命令行方式）：／path／spark-summit etlstart.py

import os, codecs
from pyspark import  SparkContext
import shutil
from datetime import datetime
from pyspark.mllib.tree import RandomForest
from pyspark.mllib.util import MLUtils

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

    with codecs.open('results.txt',"a+","utf-8") as f1:
        for item in data:
            string = ','.join(item).encode('utf8')
            print >> f1,string.decode('utf8')

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

speed = {"auto":0,"silence":1,"low":2,"mid":3,"high":4,"super":5}
direction = {"auto":0,"vdir":1,"hdir":2}
mode = {"wind":0,"cool":1,"heat":2,"auto":3,"dehu":4}


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
    
def transformer(item):
    item[0] = str(int(item[0])-24)
    item[1] = str(int(item[1])-24)
    item[2] = str(int(item[2])-24)
    item[3] = str(speed[item[3]])
    item[4] = str(speed[item[4]])
    item[5] = str(speed[item[5]])
    item[6] = str(direction[item[6]])
    item[7] = str(direction[item[7]])
    item[8] = str(direction[item[8]])
    item[9] = str(mode[item[9]])
    item[10] = str(mode[item[10]])
    item[11] = str(mode[item[11]])
  
    item[13] = str(speed[item[13]])
    item[14] = str(direction[item[14]])
    item[15] = str(mode[item[15]])
    item[16] = str(int(item[16])-24)
    return item

def labelCombination(s):
        
        classLabel = int(s[-1])*90+int(s[-2])*1+int(s[-3])*5+int(s[-4])*15       
        s.pop()
        s.pop()
        s.pop()
        s.pop()        
        s.append(str(classLabel))        
        return s
    
def myFunc(words):
#     words = s.split(",")
    return words[-1]+' 1:'+words[0]+' 2:'+words[1]+' 3:'+words[2]+' 4:'+words[3] +' 5:'+words[4]+' 6:'+words[5]+' 7:'+words[6]+' 8:'+words[7] +' 9:'+words[8]+' 10:'+words[9]+' 11:'+words[10]+' 12:'+words[11]+' 13:'+words[12]
  

    
if __name__ == "__main__":
    file_dir = "/home/xuepeng/Desktop/smarthome"
    file_weather = "/home/xuepeng/Desktop/weather/china_dec_weather.txt"
    sc = SparkContext("local[20]", "First Spark App")
    #original Data
    raw_data = sc.wholeTextFiles(file_dir)
    #weather information
    data_weather = sc.textFile(file_weather).map(lambda line: line.split(","))\
                     .map(lambda line:(line[0]+" "+line[1],line)).cache()
 
    if os.path.exists('./results.txt'):
        os.remove('./results.txt')
    if os.path.exists('./libSVMFile_results.txt'):
        os.remove('./libSVMFile_results.txt')
#     if os.path.exists('./output_weather'):#modifyed by Xueping
#         shutil.rmtree('./output_weather') #modifyed by Xueping
    raw_data.foreach(processfile)

    firstStepData = sc.textFile('./results.txt').\
                    map(lambda line: line.split(",")).\
                    filter(filterone).map(lambda line:(line[0],line)).\
                    groupByKey().mapValues(list).filter(lambda item:len(item[1]) > 3).\
                    map(lambda item: (str(len(item[1])),item[1])).\
                    flatMapValues(lambda item:item).map(addNoToSequence)
                                        
   
    secondStepData = firstStepData.map(lambda line:(line[-1]+" "+line[4][0:10],line)).union(data_weather)\
                     .groupByKey().mapValues(list).filter(filterline).map(lambda line:line[1])\
                     .flatMap(mapone).map(lambda line:(line[1]+" "+line[4],line)).sortByKey().map(lambda line:line[1])\
    
    #begin to run randomForests
    stepOneData = secondStepData.map(lambda item :(item[1],item))\
                    .groupByKey().mapValues(list).map(lambda item:item[1])\
                    .flatMap(dataPrepareStepOne)
                    
    stepOneDataLimitedTemp = stepOneData.filter(filterTemp).map(transformer).map(labelCombination).map(myFunc)
    
    with codecs.open('./libSVMFile_results.txt',"w","utf-8") as f1:
        for item in stepOneDataLimitedTemp.collect():
#             string = ','.join(item).encode('utf8')
            print >> f1,item.decode('utf8')
    
    data = MLUtils.loadLibSVMFile(sc, './libSVMFile_results.txt')
    # Split the data into training and test sets (30% held out for testing)
    (trainingData, testData) = data.randomSplit([0.7, 0.3])

    # Train a RandomForest model.
    #  Empty categoricalFeaturesInfo indicates all features are continuous.
    #  Note: Use larger numTrees in practice.
    #  Setting featureSubsetStrategy="auto" lets the algorithm choose.
    model = RandomForest.trainClassifier(trainingData, numClasses=448, categoricalFeaturesInfo={},
                                         numTrees=5, featureSubsetStrategy="auto",
                                         impurity='gini', maxDepth=10, maxBins=32)

    # Evaluate model on test instances and compute test error
    predictions = model.predict(testData.map(lambda x: x.features))
    labelsAndPredictions = testData.map(lambda lp: lp.label).zip(predictions)
    testErr = labelsAndPredictions.filter(
        lambda lp: lp[0] != lp[1]).count() / float(testData.count())
    print('Test Error = ' + str(testErr))
    print('Learned classification forest model:')


    sc.stop()