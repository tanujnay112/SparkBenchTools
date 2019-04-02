import pyspark
from pyspark import SparkContext
import math
import os


def stuff(x):
    xrdd = sc.parallelize(x.toList)
    xrdd.zipWithIndex.map(lambda (a, b): (b / 2, a)) \
        .reduceByKey(lambda a, b: abs(a - b))
    return xrdd.collect()


def folder(acc, second):
    last, soFar = acc
    uh, item = second
    if(item == -1):
        return acc
    return (item, max(soFar, abs(item - last)))


def add(a, b):
    return a + b


sc = SparkContext()
sc._jsc.hadoopConfiguration().set("fs.s3n.awsAccessKeyId", os.environ['AWS_ACCESS_KEY_ID'])
sc._jsc.hadoopConfiguration().set("fs.s3n.awsSecretAccessKey", os.environ['AWS_SECRET_ACCESS_KEY'])
text_file = sc.textFile("file:///data/cachetraces4*", 4800)
distances = text_file.filter(lambda line: len(line.split(",")) >= 2).cache();
distances = distances.map(lambda line: (line.split(",")[0], (0, int(line.split(",")[1])))).cache();
distances = distances.sortBy(lambda (x, (a, b)): b) \
    .foldByKey((0, -1), folder).mapValues(lambda (a, b): b)
#.flatMap(lambda (a, b): stuff(b))

distances.coalesce(1).saveAsTextFile("file:///data/distanceoutput.txt")

