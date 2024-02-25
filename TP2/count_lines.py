from pyspark import SparkConf, SparkContext

appName = "count_lines"
conf = SparkConf().setAppName(appName).setMaster("local[2]")
sc = SparkContext(conf=conf)



file = sc.textFile("hdfs://hadoop-master:9000/user/root/arbres.csv")

nombre = file.count()
print("number of line is:", nombre)
