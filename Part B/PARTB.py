from mrjob.job import MRJob
from mrjob.step import MRStep

class part_b(MRJob):

	''' creating the mapper1 which yields the transaction_values for their corresponding to_address fields for the transactions dataset and while checking
	another condition it yields the address field from the contracts dataset. I have yielded (2,1) for the contracts dataset inroder to avoid overlapping 
	values between both the datasets while the data is being mapped by the mapper '''

	def mapper1(self, _, line):
		fields = line.split(',')
		try:
			
			if len(fields) == 7: ''' if the length of the fields is 7, then the data is coming from the transactions dataset'''
				address_t = fields[2] ''' extracting the to_address field'''
				value = int(fields[3]) '''extracting the transaction values from the transactions dataset'''
				yield address_t, (1,value) '''yielding the to_address field along with the transaction values'''

			
			elif len(fields) == 5: ''' if the length of the fields is 5, then the data is coming from the contracts dataset'''
				address_c = fields[0] ''' extracting the address field from the contracts dataset'''
				yield address_c, (2,1) '''yielding the address field along with a 2 to differentiate between the to_address and address fields'''
		except:
			pass
	
	''' the function of the reducer1 is to check if the data is coming from transactions dataset or contracts dataset. I further check if the data 
	that is coming from the transactions dataset is also present in the contracts dataset. If this condition is true then the data is considered to be in 
	the smart contracts and hence we append those values to the smart_contracts list and yield the same along with the address value.'''

	def reducer1(self, key, values):
		f = False
		smart_contracts = []
		for i in values:
			if i[0]==1: 
				smart_contracts.append(i[1])
			elif i[0] == 2:
				f = True
		if f:
			yield key, sum(smart_contracts)

	def mapper2(self, key,value):
		yield None, (key,value)

	def reducer2(self, _, keys):
		sortedval = sorted(keys, reverse = True, key = lambda x: x[1])
		for i in sortedval[:10]:
			yield i[0], i[1]

	def steps(self):
		return [MRStep(mapper = self.mapper1, reducer=self.reducer1), MRStep(mapper = self.mapper2, reducer = self.reducer2)]

if __name__ == '__main__':
	part_b.run()
			
