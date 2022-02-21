import pyspark
from pyspark import SparkConf
from pyspark import SparkContext
conf = SparkConf()
conf.setAppName('spark-yarn-bhanu')
sc = SparkContext(conf=conf)
#cols=["path","isDirectory","length","replication","blocksize","modification_time","access_time","owner","group","permission","isSymlink","hasAcl","isEncrypted","isErasureCoded"]
fl=open("list_files.csv",'w')
#sc = pyspark.SparkContext('local[*]')
hadoop = sc._jvm.org.apache.hadoop
fs = hadoop.fs.FileSystem
conf = hadoop.conf.Configuration()
path = hadoop.fs.Path('/data')

def traverse_dir(path):
    for f in fs.get(conf).listStatus(path):
        #print(f.toString())
        #print(f.toString().split(";"))
        v={}
        for i in f.toString().split(";"):
            #print(str(i).rstrip('}').replace('HdfsNamedFileStatus{',''))
            v[str(i).rstrip('}').replace('HdfsNamedFileStatus{','').split('=')[0].strip()] = str(i).rstrip('}').replace('HdfsNamedFileStatus{','').split('=')[1]
        fl.write(f.toString()+"\n")
        print(v)
        if f.isDirectory():
            traverse_dir(f.getPath())
traverse_dir(path)
fl.close()
#for f in fs.get(conf).listStatus(path):
    #print(f.getPath(), f.getLen())
#    print(f.toString())
