from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster("local").setAppName("MinTemperatures")
sc = SparkContext(conf = conf)

def parseLine(line):
    fields = line.split(',')
    stationID = fields[0]
    entryType = fields[2]
    temperature = float(fields[3]) * 0.1 * (9.0 / 5.0) + 32.0
    return (stationID, entryType, temperature)

lines = sc.textFile("file:///D:/Github/Hadoop_Spark_Practice/udemy_pyspark/1800.csv")
parsedLines = lines.map(parseLine)  # transfer from line to parsed pairs
minTemps = parsedLines.filter(lambda x: "TMAX" in x[1])  # only keep the records with TMIN
stationTemps = minTemps.map(lambda x: (x[0], x[2]))  # remove x[1]
minTemps = stationTemps.reduceByKey(lambda x, y: max(x,y))  # find the min value
results = minTemps.collect()

for result in results:
    print(result[0] + "\t{:.2f}F".format(result[1]))
