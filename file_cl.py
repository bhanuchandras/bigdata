import pyspark
from pyspark import SparkConf
from pyspark import SparkContext
from pyspark.sql import SparkSession

conf = SparkConf()
conf.setAppName('spark-yarn-bhanu')
sc = SparkContext(conf=conf)
spark = SparkSession.builder.appName('spark-yarn-bhanu').getOrCreate()

#cols=["path","isDirectory","length","replication","blocksize","modification_time","access_time","owner","group","permission","isSymlink","hasAcl","isEncrypted","isErasureCoded"]
fl=open("list_files.csv",'w')
#sc = pyspark.SparkContext('local[*]')
hadoop = sc._jvm.org.apache.hadoop
fs = hadoop.fs.FileSystem
conf = hadoop.conf.Configuration()
path = hadoop.fs.Path('/data')
ll=[]
def traverse_dir(path):
    for f in fs.get(conf).listStatus(path):
        #print(f.toString())
        #print(f.toString().split(";"))
        v={}
        for i in f.toString().split(";"):
            #print(str(i).rstrip('}').replace('HdfsNamedFileStatus{',''))
            v[str(i).rstrip('}').replace('HdfsNamedFileStatus{','').split('=')[0].strip()] = str(i).rstrip('}').replace('HdfsNamedFileStatus{','').split('=')[1]
        #print(v)
	ll.append(v)
        if f.isDirectory():
            traverse_dir(f.getPath())
traverse_dir(path)
print(ll)
dataframe = spark.createDataFrame(ll)
dataframe.show()
#for f in fs.get(conf).listStatus(path):
    #print(f.getPath(), f.getLen())
#    print(f.toString())
