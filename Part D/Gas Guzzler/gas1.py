from mrjob.job import MRJob
import time

''' 
In the mapper, I extract the gas values for every month and year pairs in our dataset. Month and year are extracted separately and then joined togetehr and used as a key for the reducer.
'''

''' The reducer calculates the average value of gas per month by adding all the gas values and dividing it by the total number of values. 
Fnially the reducer yields the average value of gas per month/year.
'''

class gas_guzzler(MRJob):
    def mapper(self, _, lines):
        try:
            fields = lines.split(",")
            if len(fields) == 7:
                gas_val = int(fields[5])
                date = time.gmtime(float(fields[6]))
                year_mon = str(date.tm_year) + '-' + str(date.tm_mon)
                yield(year_mon,gas_val)
        except:
            pass
    
    def reducer(self, key, values):  
        price_var = 0
        c = 0

        for val in values:
            price_var = price_var + val
            c = c + 1

        yield(key, float(price_var/c))


if __name__ == '__main__':
    gas_guzzler.run()
