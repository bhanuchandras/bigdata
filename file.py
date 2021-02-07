import pyspark
sc = pyspark.SparkContext('local[*]')
hadoop = sc._jvm.org.apache.hadoop
fs = hadoop.fs.FileSystem
conf = hadoop.conf.Configuration()
path = hadoop.fs.Path('/tmp')
def traverse_dir(path):
    for f in fs.get(conf).listStatus(path):
        print(f.toString())
        if f.isDirectory():
            traverse_dir(f.getPath())

traverse_dir(path)
#for f in fs.get(conf).listStatus(path):
    #print(f.getPath(), f.getLen())
#    print(f.toString())
