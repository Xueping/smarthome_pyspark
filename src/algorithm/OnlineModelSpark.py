'''
Created on 11 May 2017

@author: xuepeng
'''

from pyspark import SparkContext
import numpy as np
import math
import random



def predict(inst):
    
    def getVotesForInstance(inst):
        votes = np.zeros(numClasses)
        sum_weight = 0;
        
        for i in range(len(votes)):
            votes[i] = prediction(inst, i);
            sum_weight += prediction(inst, i)
            
        try:
            for i in range(len(votes)):
                votes[i] /= sum_weight
        
        except ZeroDivisionError:
            print "Can't normalize array. Sum is zero."            
        
        return votes;
    
    return np.argmax(getVotesForInstance(inst)) == int(inst[-1])


def perceptron(numClasses,numFeatures,inst):
    weight = bcWeights.value
    
    def prediction(inst, classVal):
        sums = 0.0;
        for i in range(numFeatures - 1):
            sums += weight[classVal][i] * float(inst[i]);
        
        sums += weight[classVal][numFeatures - 1];
        return 1.0 / (1.0 + math.exp(-sums));
    
    def getVotesForInstance(inst):
        votes = np.zeros(numClasses)
        sum_weight = 0;
        
        for i in range(len(votes)):
            votes[i] = prediction(inst, i);
            sum_weight += prediction(inst, i)
            
        try:
            for i in range(len(votes)):
                votes[i] /= sum_weight
        
        except ZeroDivisionError:
            print "Can't normalize array. Sum is zero."            
        
        return votes;
    
    def correctlyClassifies(inst):
        return np.argmax(getVotesForInstance(inst)) == int(inst[-1])
    

    
    preds = np.zeros(numClasses);
    for i in range(numClasses):
        preds[i] = prediction(inst, i);

    actualClass = int(inst[-1]);
    for i in range(numClasses):
        actual = 1.0 if(i == actualClass) else 0.0;
        delta = (actual - preds[i]) * preds[i] * (1 - preds[i]);
        for j in range(numFeatures):
            weight[i][j] += learningRatio * delta * float(inst[j]);
        
        weight[i][numFeatures - 1] += learningRatio * delta;
    
    print weight
    
#     bcWeights = sc.broadcast(weight)





if __name__ == "__main__":

    numFeatures = 57
    numClasses = 5
    weights= np.zeros((numClasses,numFeatures))
    learningRatio = 1.0
    
    #Initialize weight
    for i in range(numClasses):
        for j in range(numFeatures):
            weights[i][j] = 0.2 * random.random() - 0.1;

    sc = SparkContext("local[20]", "NetworkWordCount")
    
    bcWeights = sc.broadcast(weights)
    
    distFile = sc.textFile("data_mode.csv")
    instances = distFile.map(lambda item : item.split(","))
    
    
    instances.foreach(lambda inst:perceptron(numClasses,numFeatures,inst))
# 
#     print bcWeights.value
