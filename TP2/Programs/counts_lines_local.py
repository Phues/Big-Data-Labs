from pyspark import SparkConf, SparkContext

app = "count_lines_local"
conf = SparkConf().setAppName(app)
sc = SparkContext(conf=conf)

brut = sc.textFile("file:///root/arbres.csv")
nb = brut.count()
print("Le nombre de lignes du RDD:", nb)

