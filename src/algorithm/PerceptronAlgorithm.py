'''
Created on 11 May 2017

@author: xuepeng
'''

import math
import random
import numpy as np


class PerceptronAlgorithmSpark(object):
    
    def __init__(self,numFeatures, numClasses,learningRatio):
        self._numFeatures = int(numFeatures)
        self._numClasses = int(numClasses)
        self._learningRatio = learningRatio
        self._weightAttribute = np.zeros((self._numClasses, self._numFeatures))
        
        
    def getWeight(self):
        return self._weightAttribute
    
    def initWeight(self):#Initialize weight
        for i in range(self._numClasses):
            for j in range(self._numFeatures):
                self._weightAttribute[i][j] = 0.2 * random.random() - 0.1

    def prediction(self, inst, classVal):
        sums = 0.0;
        for i in range(self._numFeatures - 1):
            sums += self._weightAttribute[classVal][i] * float(inst[i]);
        
        sums += self._weightAttribute[classVal][self._numFeatures - 1];
        return 1.0 / (1.0 + math.exp(-sums));
    
    
    def getVotesForInstance(self,inst):
        votes = np.zeros(self._numClasses)
        sum_weight = 0;
        
        for i in range(len(votes)):
            votes[i] = self.prediction(inst, i);
            sum_weight += self.prediction(inst, i)
            
        try:
            for i in range(len(votes)):
                votes[i] /= sum_weight
        
        except ZeroDivisionError:
            print "Can't normalize array. Sum is zero."            
        
        return votes;
    
    def predict(self,inst):
        return np.argmax(self.getVotesForInstance(inst)) == int(inst[-1])

    def train(self,rdd):
        
        numberSamplesCorrect = 0
        numberSamples = 0
        
        instances = rdd.map(lambda item : item.split(",")).collect()
        for inst in instances:
            
            if self.predict(inst):
                numberSamplesCorrect += 1
            numberSamples += 1  
            
            preds = np.zeros(self._numClasses);
            for i in range(self._numClasses):
                preds[i] = self.prediction(inst, i);
    
            actualClass = int(inst[-1]);
            for i in range(self._numClasses):
                actual = 1.0 if(i == actualClass) else 0.0;
                delta = (actual - preds[i]) * preds[i] * (1 - preds[i]);
                for j in range(self._numFeatures):
                    self._weightAttribute[i][j] += self._learningRatio * delta * float(inst[j]);
                
                self._weightAttribute[i][self._numFeatures - 1] += self._learningRatio * delta 
       
        print  self._weightAttribute 
        accuracy = 100.0 * numberSamplesCorrect / numberSamples;
        print(str(numberSamples) + "_instances-proccssed_with " + str(accuracy) + " % accuracy");   
    
    def trainOn(self, dstream):

        def update(rdd):
            # LinearRegressionWithSGD.train raises an error for an empty RDD.
            if not rdd.isEmpty():
                self.train(rdd)

        dstream.foreachRDD(update)
            
        