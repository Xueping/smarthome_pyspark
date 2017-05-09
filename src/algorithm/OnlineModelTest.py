'''
Created on 9 May 2017

@author: xuepeng
'''

    
import csv

# from pyspark.context import SparkContext
# 
from onlineModel import OnlineModel
# 
# 
if __name__ == "__main__":
#     
    om = OnlineModel(numFeatures=57, numClasses=5,learningRatio=1.0)
#     
#     sc = SparkContext("local[20]", "PythonRandomForestClassificationExample")
#     distFile = sc.textFile("data_mode.csv")
#     instances = distFile.map(lambda item : item.split(",")).collect()
#     
    numberSamplesCorrect = 0
    numberSamples = 0
#         
#     for inst in instances:
#         if om.correctlyClassifies(inst):
#             numberSamplesCorrect += 1
#         om.train(inst)
#         numberSamples += 1  
#     
#     accuracy = 100.0 * numberSamplesCorrect / numberSamples;
#     print(str(numberSamples) + "_instances-proccssed_with " + str(accuracy) + " % accuracy");   
    

    with open('data_temp.csv', 'rb') as csvfile:
        spamreader = csv.reader(csvfile, delimiter=',', quotechar='|')
        for inst in spamreader:
            if om.correctlyClassifies(inst):
                numberSamplesCorrect += 1
            om.train(inst)
            numberSamples += 1  
     
    accuracy = 100.0 * numberSamplesCorrect / numberSamples;
    print(str(numberSamples) + "_instances-proccssed_with " + str(accuracy) + " % accuracy");   
    
            