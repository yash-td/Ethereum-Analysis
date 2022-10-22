from mrjob.job import MRJob
import time

class number_of_transactions(MRJob):

	def mapper(self, _, line):

		''' Reading all the fields of by splitting our data using a comma (extracting fields from the csv data file) '''
		total_fields = line.split(",")
		try:
			if (len(total_fields) == 7):

				''' Extracting the time field from our data'''
				time_value_epochs = int(total_fields[6])

				''' using the time.gmttime method from the time library in python, extracting the month and year values '''
				year = time.strftime("%Y", time.gmtime(time_value_epochs))
				month = time.strftime("%m", time.gmtime(time_value_epochs))

				''' yielding the mapper output as ((year,month), 1) in order to count all the number of transactions '''
				yield((year, month), 1)
		except:
			pass

	def combiner(self, keys, value):
		yield(keys, sum(value))

	''' yielding the sum of the extracted values from the mapper year and monthwise in the reducer '''
	def reducer(self, keys, value):
		yield(keys, sum(value))

if __name__ =='__main__':
	number_of_transactions.run()
