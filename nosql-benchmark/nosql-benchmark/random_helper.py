from random import randint

class RandomHelper():
    
    def integer(minValue=0, maxValue=9):        
        return randint(int(minValue), int(maxValue))    