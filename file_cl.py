import pyspark
from pyspark import SparkConf
from pyspark import SparkContext
conf = SparkConf()
conf.setAppName('spark-yarn-bhanu')
sc = SparkContext(conf=conf)

#sc = pyspark.SparkContext('local[*]')
hadoop = sc._jvm.org.apache.hadoop
fs = hadoop.fs.FileSystem
conf = hadoop.conf.Configuration()
path = hadoop.fs.Path('/data')
def traverse_dir(path):
    for f in fs.get(conf).listStatus(path):
        print(f.toString())
        if f.isDirectory():
            traverse_dir(f.getPath())

traverse_dir(path)
#for f in fs.get(conf).listStatus(path):
    #print(f.getPath(), f.getLen())
#    print(f.toString())
