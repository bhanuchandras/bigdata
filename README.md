# bigdata

spark-submit --master yarn  file_cl.py

sudo -u hdfs hdfs dfs -chmod 777  /user
sudo -u hdfs hdfs dfs -mkdir /data
sudo -u hdfs hdfs dfs -chmod 777  /data

sudo su
cd /var/log/
hdfs dfs -put cloudera-scm-server /data
hdfs dfs -put spark /data
hdfs dfs -put hadoop-hdfs /data
