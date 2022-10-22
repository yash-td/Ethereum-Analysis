from mrjob.job import MRJob
import time

class cal_average_a2(MRJob):

    def mapper(self,_,line):
        try:
            '''
            Reading all the fields of by splitting our data using a comma (extracting fields from the csv data file)
            '''
            fields = line.split(',') 
            '''
            Extracting the value of transaciton field from our data
            '''
            val = float(fields[3])
            '''
            Extracting the time field from our data
            '''
            date = time.gmtime(float(fields[6]))
            '''
            yielding the mapper output as ((month,year), (1,val)) to get the number of transactions along with the transaction values
            ''' 
            yield ((date.tm_mon,date.tm_year), (1, val))

        except:
            pass

    '''
    Counting the number of transactions and summing all the tranaction values together monthwise
    '''
    def combiner(self,key,val):
        count = 0
        total = 0
        for v in val:
            count+= v[0]
            total+= v[1]

        yield (key,(count,total))

    def reducer(self,key,val):
        count = 0
        total = 0
        for v in val:
            count += v[0]
            total += v[1]
        '''
        Finally yielding the average values by diving the total sum of values divided by the total number of transactions
        '''
        yield (key, float(total/count))

if __name__=='__main__':
    cal_average_a2.run()




