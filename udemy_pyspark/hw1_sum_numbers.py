from pyspark import SparkConf, SparkContext
conf = SparkConf().setMaster('local').setAppName('Homework1')
sc = SparkContext(conf=conf)


def split_line(line):
    splitted = line.split(',')
    return tuple([int(splitted[0]), float(splitted[2])])


input_ = sc.textFile('file:///D:/Github/Hadoop_Spark_Practice/udemy_pyspark/customer-orders.csv')
customer_amount = input_.map(split_line)
sum_amount = customer_amount.reduceByKey(lambda x, y: x+y)
sorted_sum = sum_amount.map(lambda x: (x[1], x[0])).sortByKey()

results = sorted_sum.collect()

for result in results:
    print(result[1], ':', result[0])
