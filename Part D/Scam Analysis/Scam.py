import pyspark
import json
from pyspark.sql.functions import *


def clean(line):
    try:
        fields = line.split(',')
        ''' Checking if the input dataset's every row has 7 fields (removing rows with empty fields)'''
        if len(fields) == 7:  
            str(fields[2])
            ''' Removing all the rows where mine value is 0 ''' 
            if int(fields[3]) == 0:
                return False
            return True
    except:
        return False

''' Creating a spark context'''
sc = pyspark.SparkContext()
print(sc.applicationId)

''' Creating an SQL context from Spark Context''' 
sql_context = pyspark.SQLContext(sc)

''' Reading the scam csv file (converted from the scam.json file on hadoop) using the SQL context'''
dat = sql_context.read.option('header', False).format('csv').load("scam.csv")

''' After reading the scams csv file, we convert it to spark's rdd for further spark operations'''
sc_rdd = dat.rdd.map(lambda x: (x[0],(x[1],x[2])))

''' We filter our transactions data using the above defined clean function '''
trans_data = sc.textFile('/data/ethereum/transactions').filter(clean)

''' Extracting data from the transactions dataset [2] is 'to_address' [4] is 'gas' and [3] is 'value' '''
extract_trans = trans_data.map(lambda l:  (l.split(',')[2], (float(l.split(',')[4]), float(l.split(',')[3]))))

''' joining the scams.csv dataset with our trasactions dataset '''
scam_join = extract_trans.join(sc_rdd)

''' x[1] is the key and x[1][0] is the data from transactions dataset and x[1][1] is the data from the scams dataset. 
 In the below map funciton we have extracred the scam_type which is x[1][1][0], the gas value x[1][0][0] and the mine value x[1][0][1]'''
scam_extract = scam_join.map(lambda x: (x[1][1][0], (x[1][0][0], x[1][0][1])))

''' Using the below reduceByKey method we have performed the summation of all the values from the above dataset'''
scam_sum= scam_extract.reduceByKey(lambda a,b: (a[0]+b[0], a[1]+b[1]))
scam_sum.saveAsTextFile('Output1')

'''
Part 2
'''
map_new = scam_join.map(lambda x: (x[1][1],  x[1][0][0]))
reduced_new = map_new.reduceByKey(lambda a,b: a+b).sortByKey(ascending=True)
reduced_new.saveAsTextFile('Output2')
