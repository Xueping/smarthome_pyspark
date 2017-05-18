'''
Created on 11 May 2017

@author: xuepeng
'''
import math
import random
import numpy as np


class PerceptronModel(object):
    
    def __init__(self,numFeatures, numClasses,learningRatio):
        self._numFeatures = int(numFeatures)
        self._numClasses = int(numClasses)
        self._learningRatio = learningRatio
        self._weightAttribute = np.zeros((self._numClasses, self._numFeatures))
        
    def initWeight(self):#Initialize weight
        for i in range(self._numClasses):
            for j in range(self._numFeatures):
                self._weightAttribute[i][j] = 0.2 * random.random() - 0.1
    

    def numFeatures(self):
        return self._numFeatures

    def numClasses(self):
        return self._numClasses
    
    def weights(self):
        return self._weightAttribute
    
    def learningRatio(self):
        return self._learningRatio
    
    def setWeights(self,weight):
        self._weightAttribute = weight
    
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
    
    def prediction(self, inst, classVal):
        sums = 0.0;
        for i in range(self._numFeatures - 1):
            sums += self._weightAttribute[classVal][i] * float(inst[i]);
        
        sums += self._weightAttribute[classVal][self._numFeatures - 1];
        return 1.0 / (1.0 + math.exp(-sums));
    
    def predict(self,inst):
        return np.argmax(self.getVotesForInstance(inst)) == int(inst[-1])
    
    
    def train(self,inst):
        preds = np.zeros(self._numClasses);
        for i in range(self._numClasses):
            preds[i] = self.prediction(inst, i);

        actualClass = int(inst[-1]);
        for i in range(self._numClasses):
            actual = 1.0 if(i == actualClass) else 0.0;
            delta = (actual - preds[i]) * preds[i] * (1 - preds[i]);
            for j in range(self._numFeatures):
                self._weightAttribute[i][j] += self._learningRatio * delta * float(inst[j]);
            
            self._weightAttribute[i][self._numFeatures - 1] += self._learningRatio * delta;
  
    
    