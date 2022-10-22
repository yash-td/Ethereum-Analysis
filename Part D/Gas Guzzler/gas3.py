import pyspark
import time

global topadd # creating a global address variable to store the top three contracts from the for loop and use it in the 

top_three_contracts = ["0xaa1a6e3e6ef20068f7f8d8c835d2d22fd5116444", "0xfa52274dd61e1643d2205169732f29114bc240b3","0x7727e5113d1d161373623e5f49fd568b4f543a9e"]

'''
creating a function to clean our transactions dataset and adding a condition which only takes a particular value of address from our list of top three contracts
'''
def filter_trans(line):
    try:
        fields = line.split(',')
        if len(fields) != 7:
            return False
        # if fields[2] in popular_contracts:
        #     return True
        if fields[2] == topadd:
            return True
    except:
        return False

'''
creating a function to clean our blocks dataset
'''

def filter_blocks(line):
    try:
        fields = line.split(',')
        if len(fields)!=9:
            return False
        float(fields[0])
        float(fields[6])
        float(fields[7])
        return True
    except:
        return False



for add in top_three_contracts:
    '''
    Creating a for loop which will extract the difficulty of mining per month for the top three contracts (the list that is created above)
    '''
    sc = pyspark.SparkContext()
    print(sc.applicationId)
    topadd = add
    trans = sc.textFile('/data/ethereum/transactions')
    clean_trans = trans.filter(trans_good)
    block_id = clean_trans.map(lambda l: (l.split(',')[0], l.split(',')[2])) # extracting block id and address from the transactions data


    blocks = sc.textFile('/data/ethereum/blocks')
    clean_blocks = blocks.filter(filter_blocks)
    '''
    extracting block-id, difficulty and year-month pairs from the blocks dataset
    '''
    block_mapped = clean_blocks.map(lambda l: (l.split(',')[0], (int(l.split(',')[3]), time.strftime("%Y-%m", time.gmtime(float(l.split(',')[7])))))) 

    joined = block_mapped.join(block_id) # joining both the datasets with same block-id
    extracted = joined.map(lambda x: (x[1][0][1], x[1][0][0] )) # extracting the year-month pairs and difficulty for all those blocks 

    summed = extracted.reduceByKey(lambda a,b: float(a+b)).sortByKey(ascending=True) # taking the summation of difficulty

    summed.saveAsTextFile('g3_'+topadd)
    sc.stop() # stopping the instance after every for loop iteration 
