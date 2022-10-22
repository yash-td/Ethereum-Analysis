import pyspark
import time

'''
creating a function to clean our contracts dataset from where we will extract the block number
'''
def clean_contracts(line):
    try:
        fields = line.split(',')
        if len(fields) != 5:
            return False
        float(fields[3])
        return True
    except:
        return False

'''
creating a function to clean our blocks dataset
'''

def clean_blocks(line):
    try:
        fields = line.split(',')
        if len(fields)!=9:
            return False
        float(fields[0]) # block number
        float(fields[6]) # gas used
        float(fields[7]) # time stamp
        return True
    except:
        return False

sc = pyspark.SparkContext() # creating spark context
print(sc.applicationId)

read_contract = sc.textFile('/data/ethereum/contracts')
contract_clean = read_contract.filter(clean_contracts) # cleaning the contracts dataset
block_id = contract_clean.map(lambda l: (l.split(',')[3], 1)) # getting block id from the contracts dataset

read_block = sc.textFile('/data/ethereum/blocks')
blocks_clean = read_block.filter(clean_blocks) # cleaning the blocks dataset using the function we created above

'''
Extracting block number, difficulty, gas used and year-month pairs from the blocks dataset
'''
block_map = blocks_clean.map(lambda l: (l.split(',')[0], (int(l.split(',')[3]),int(l.split(',')[6]), time.strftime("%Y-%m", time.gmtime(float(l.split(',')[7]))))))
print(block_map.take(2))
# [(u'4776199', (1765656009004680, 2042230, '2017-12')), (u'4776200', (1765656009037448, 4385719, '2017-12'))]


'''
Joining the contracts and the blocks dataset using the key value as the block number.
We get block id, along with difficulty, gas used and date which are common in both our datasets. We also have a '1' at the position x[1][1],
from our block_id
'''
results = block_map.join(block_id)
print(results.take(2))
# [(u'7352442', ((1862016659491710, 6317667, '2019-03'), 1)), (u'7352442', ((1862016659491710, 6317667, '2019-03'), 1))]

'''
Extracting Year-Month pairs, difficulty, gas value and a '1' which will help us keep a count of the values
'''
result1 = results.map(lambda x: (x[1][0][2], (x[1][0][0], x[1][0][1], x[1][1])))
print(result1.take(2))
# [('2017-08', (1600249491213932, 2032838, 1)), ('2019-06', (2134143517862686, 7989075, 1))]

'''
Adding all the values
Now we have total values of difficulty and gas price per year-month pairs along with their counts
'''
total_val = result1.reduceByKey(lambda a,b: (a[0]+b[0], a[1]+b[1], a[2]+b[2]))
print(total_val.take(2))
# [('2019-02', (1084082528199181398629L, 3197663942850, 419164)), ('2016-07', (1662703037969682192, 43543836232, 28773))]

'''
Dividing the difficulty and gas price values by the total counts to get average if these values per year-month pairs
'''
avg_val = total_val.map(lambda x: (x[0], (float(x[1][0] / x[1][2]), float(x[1][1] / x[1][2])))).sortByKey(ascending=True)
print(avg_val.take(2))
# [('2015-08', (4030960805570, 360218)), ('2015-09', (6577868584193, 540131))]

'''
saving our output as g2_out
'''
avg_val.saveAsTextFile('g2_out')
#Output is of the format YY-MM, Complexity, Avg Gas