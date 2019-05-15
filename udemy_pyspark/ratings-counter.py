from pyspark import SparkConf, SparkContext  # import ralated packages
import collections

conf = SparkConf().setMaster("local").setAppName("RatingsHistogram")
#         run on local machine single thread, set app name for app ui
sc = SparkContext(conf=conf)
# spark content object

lines = sc.textFile("file:///D:/Github/Hadoop_Spark_Practice/udemy_pyspark/ml-100k/u.data")
# read file
ratings = lines.map(lambda x: x.split()[2])
# new RDD calls rating
result = ratings.countByValue()

sortedResults = collections.OrderedDict(sorted(result.items()))
for key, value in sortedResults.items():
    print("%s %i" % (key, value))
