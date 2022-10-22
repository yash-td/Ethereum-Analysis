import pyspark
import datetime
import time

blocks = [2463000,4370000] # block number of the forks

forks = [ [datetime.datetime(2016,10,18),'Tangerine whistle'], [datetime.datetime(2017,10,16),'Byzantium'] ] # date of fork and name of fork

'''
Tangerine whistle
Oct-18-2016 01:19:31 PM +UTC
#2463000
'''

'''
Byzantium

Oct-16-2017 05:22:11 AM +UTC
Block number: 4,370,000
ETH price: $334.23 USD
ethereum.org on waybackmachine

'''
def clean_trans(line):
    try:
        fields = line.split(',')
        block_ts = datetime.datetime.fromtimestamp(int(fields[6])) # extracting block timestamp
        date_trans = time.gmtime(float(fields[6])) 
        add = fields[2]
        gas_val = int(fields[5])

        check_fork = False # will toggle this true when the block time is 7 days before or after the fork date.

        for fork in forks:
            start_date = fork[0] + datetime.timedelta(-7)
            end_date = fork[0] + datetime.timedelta(7)
            print(start_date, end_date)

            if block_ts > start_date and block_ts < end_date: # check if 
                fork_name = fork[1]
                print(fork_name)
                check_fork = True 
                break
        if check_fork:
            return True

    except Exception as e:
        print(str(e))
        return False

def map_data(line):

    try:
        fields = line.split(',')
        block_ts = datetime.datetime.fromtimestamp(int(fields[6]))
        date_trans = time.gmtime(float(fields[6]))
        add = fields[2]
        # tvalue = int(fields[3])
        gas_val = int(fields[5])
        print(block_ts)

        check_fork = False

        for fork in forks:
            start_date = fork[0] + datetime.timedelta(-7)
            end_date = fork[0] + datetime.timedelta(7)
            print(start_date, end_date)

            if block_ts > start_date and block_ts < end_date:
                fork_name = fork[1]
                print(fork_name)
                check_fork = True

        if check_fork:
            datekey = str(date_trans.tm_year)+'-'+str(date_trans.tm_mon)+'-'+str(date_trans.tm_mday)
            return((datekey, add ), (gas_val,fork_name, 1))

    except Exception as e:
        print(str(e))

sc = pyspark.SparkContext()
print(sc.applicationId)

transactions = sc.textFile('/data/ethereum/transactions').filter(clean_trans) # extracts only those transactions whose block time stamp is in the range of +- 7 days of the fork date

map_trans = transactions.map(map_data) # maps data with date as the key and returns address, gas value, fork name and a '1' to count the addresses
print(map_trans.take(2))
# [(('2016-10-15', u'0x873144b9c49332a7fad5a07efb37c4c565e4fbd1'), (70153218482, 'Tangerine whistle', 1))] 
get_gas = map_trans.map(lambda x: ((x[1][1], x[0][0]), (x[1][0], x[1][2]))) # 
print(get_gas.take(2))
# [(('Tangerine whistle', '2016-10-15'), (70153218482, 1)), (('Tangerine whistle', '2016-10-15'), (63255441070, 1))]
# [(('Tangerine whistle', '2016-10-23'), (961528182055074, 35364)), (('Byzantium', '2017-10-12'), (8908169232568344, 363411))]

sum_gas = get_gas.reduceByKey(lambda x,y: (x[0]+y[0], x[1]+y[1])) # summing the gas values for all the transactions of each block ids
print(sum_gas.take(2))

sort_map = sum_gas.map(lambda x: (x[0], float(x[1][0] / x[1][1]))).sortByKey(ascending = True) # sorting the values in ascending order and getting average

sort_map.saveAsTextFile('fork_out')