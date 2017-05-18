'''
Created on 9 May 2017

@author: xuepeng
'''

    
import csv
import random

from algorithm.PerceptronModel import PerceptronModel
from onlineModel import OnlineModel


# from pyspark.context import SparkContext
# 
def OnlineModelTest():
    om = OnlineModel(numFeatures=57, numClasses=5,learningRatio=1.0)
   
    numberSamplesCorrect = 0
    numberSamples = 0 
    
    with open('data_temp.csv', 'rb') as csvfile:
        spamreader = csv.reader(csvfile, delimiter=',', quotechar='|')
        for inst in spamreader:
            if om.correctlyClassifies(inst):
                numberSamplesCorrect += 1
            om.train(inst)
            numberSamples += 1  
     
    accuracy = 100.0 * numberSamplesCorrect / numberSamples
    print(str(numberSamples) + "_instances-proccssed_with " + str(accuracy) + " % accuracy")
    
    
def perceptronModelTest():
    pm = PerceptronModel(numFeatures=110, numClasses=5,learningRatio=1.0)
    pm.initWeight()
     
    numberSamplesCorrect = 0
    numberSamples = 0
    data = []
    
    with open('data/data_mode_5.csv', 'rb') as csvfile:
        spamreader = csv.reader(csvfile, delimiter=',', quotechar='|')
        for inst in spamreader:
            data.append(inst)
    
    random.shuffle(data)
    splitIndex = int(len(data) * 0.7)
    
    train_data = data[:splitIndex]
    test_data = data[splitIndex:]
    
    #training
    for inst in train_data:
        pm.train(inst)
   
    #testing
    for inst in test_data:
        if pm.predict(inst):
            numberSamplesCorrect += 1
        numberSamples += 1  
     
    accuracy = 100.0 * numberSamplesCorrect / numberSamples;
    print(str(numberSamples) + "_instances-proccssed_with " + str(accuracy) + " % accuracy");  
    
    #testing and training at the same time
#     with open('data_temp.csv', 'rb') as csvfile:
#         spamreader = csv.reader(csvfile, delimiter=',', quotechar='|')
#         for inst in spamreader:
#             if pm.predict(inst):
#                 numberSamplesCorrect += 1
#             pm.train(inst)
#             numberSamples += 1  
#       
#     accuracy = 100.0 * numberSamplesCorrect / numberSamples;
#     print(str(numberSamples) + "_instances-proccssed_with " + str(accuracy) + " % accuracy");  
        
 
if __name__ == "__main__":
#     
    perceptronModelTest()
#     OnlineModelTest()
    
            