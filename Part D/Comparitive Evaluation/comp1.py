import pyspark
from time import *
'''
Function to clean our transactions dataset
'''
def clean_trans(line):
	try:
		fields = line.split(",")
		if len(fields) == 7:
			int(fields[3])
			return True
		else:
			return False
	except:
		return False

'''
Function to clean our contracts dataset
'''
def clean_contracts(line):
	try:
		fields = line.split(",")
		if len(fields) == 5:
			fields[0]
			return True
		else:
			return False
	except:
		return False

start = time()  # creating a time object to calculate the execution time

sc = pyspark.SparkContext()
print(sc.applicationId)
trans = sc.textFile("/data/ethereum/transactions")
transactions = trans.filter(clean_trans).map(lambda x: x.split(",")) # cleaning the transactions dataset and splitting the text file by a comma to get separate field values
features_t = trasactions.map(lambda x: (x[2], int(x[3]))) # extracting the transaction address and the transaction values 
result_t = features_t.reduceByKey(lambda a,b: a+b) # summing the values 

contract = sc.textFile("/data/ethereum/contracts")
contracts = contract.filter(clean_contracts).map(lambda x: x.split(",")) # cleaning the contracts dataset and splitting the text file by a comma to get separate field values
features_c = contracts.map(lambda x: (x[0], None)) # extracting the address field from the contracts dataset

result_c = result_t.join(features_c) # with address as the key, joining both the datasets (with only extracted values) 

# (address, (summed_value, None))
'''
This means that we have extracted smart contarcts. (Addresses that are present in both the transactions and the contracts dataset)
'''
ten = result_c.takeOrdered(10, key = lambda x: -x[1][0]) # sorting the extracted smart contracts and their corresponsding values in descending order and extracting top 10 values
top_ten = sc.parallelize(ten).saveAsTextFile("spark_3") # saving our output file

stop = time() # finished the execution of our code and extracting total time spent in the job
print('Time:', (stop-start))