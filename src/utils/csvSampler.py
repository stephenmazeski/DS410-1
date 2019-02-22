import csv
import random
import sys


def converter(inputFile, outputFile, sampleNum):
	samples = []
	n_samples = int(sampleNum)
	
	with open(inputFile, 'r') as csvfile:
		reader = csv.reader(csvfile, delimiter=',', quotechar='|')
		count = 0

		for row in reader:
			if count < n_samples:
				samples.append(row)
			elif random.random() < n_samples * 1. / (count+1):
				samples[random.randint(0, n_samples-1)] = row
			count += 1
			
	with open(outputFile, 'w') as f:
		writer = csv.writer(f)
		writer.writerows(samples)


converter(sys.argv[1], sys.argv[2], sys.argv[3])

# argv[1] : INPUT FILE LOCATION
# argv[1] : OUTPUT FILE LOCATION
# argv[1] : SAMPLE NUMBER