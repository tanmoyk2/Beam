from datetime import datetime
import calendar
import time

def addTime(counter):
    unixtime=int(time.time())+int(counter)
    return unixtime
  
#input_file='storedata.csv'
def CreateNewFile(input_file,output_file):
    with open(input_file) as f:
        lines=f.readlines()
    counter=1
    for line in lines:
        currTime=addTime(counter)
        counter+=1
        line=line.strip()+','+str(currTime)
        with open(output_file,'a') as f:
            f.write('{}\n'.format(line))
