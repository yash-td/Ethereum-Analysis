from mrjob.job import MRJob
from mrjob.step import MRStep

class top10miners(MRJob):
	''' 
	The mapper1 yields the miner size for the corresponding miner address. First I check if the number of columns in the dataset that I have passed are equal to 9.
	This means that we are using the correct dataset i.e the 'blocks' dataset which has 9 fields. The miner field which is the miner address can be found at the 
	index number 2 (3rd field) whereas the size field which is the miner sizer can be found at the index number 4 (5th column). After fetching both of these columns,
	the mapper 1 yields (miner_address, int(mine_size))
	'''
	def mapper1(self, _, line):
		fields = line.split(',')
		try:
			if len(fields) == 9:
				miner_address = fields[2]
				mine_size = fields[4]
				yield (miner_address, int(mine_size))

		except:
			pass

	'''
	The reducer1 yields the same fields as the mapper1 but instead of the size mined for each address value it yields the sum of the ether mined for every miner.
	'''
	def reducer1(self, key, value):
		try:
			yield(key, sum(value))

		except:
			pass

	'''
	I have used the mapper2 to yield the miner address and its total mine size together as a single tuple.
	'''

	def mapper2(self, key, sum_of_value):
		try:
			yield(None, (key,sum_of_value))
		except:
			pass

	'''
	In the reducer2 I have sorted the value of the mine size for its corresponding miner in descending order (reverse=True). Further I have just extracted
	the top 10 miner,mine_size pairs.
	'''
	def reducer2(self, _, value):
		try:
			sort_val = sorted(value, reverse = True, key = lambda x:x[1])
			for val in sort_val[:10]:
				yield(val[0],val[1])
		except:
			pass

	def steps(self):
		return [MRStep(mapper = self.mapper1, reducer=self.reducer1), MRStep(mapper = self.mapper2, reducer = self.reducer2)]

if __name__ == '__main__':
	top10miners.run()