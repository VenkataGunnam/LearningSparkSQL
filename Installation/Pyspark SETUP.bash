# Run this file in the Ubuntu Environment to Install pyspark

### Update existing list of packages
sudo apt update 


###############################
####    Python Installation ###
###############################


### Install python3 using pip
sudo apt install python3-pip


### Install virtual environment 
sudo apt install python3-venv

/* 
The venv module provides support for creating lightweight “virtual environments” with their own site directories,
optionally isolated from system site directories. Each virtual environment has its own Python binary (which matches 
the version of the binary that was used to create this environment) and can have its own independent set of installed 
Python packages in its site directories.

Creation of virtual environments is done by executing the command venv:
python3 -m venv /path/to/new/virtual/environment
Running this command creates the target directory (creating any parent directories that don’t exist already) and places 
a pyvenv.cfg file in it with a home key 
pointing to the Python installation from which the command was run (a common name for the target directory is .venv). 
It also creates a bin (or Scripts on Windows) subdirectory containing a copy/symlink of the Python binary/binaries (as appropriate
for the platform or arguments used at environment creation time). It also creates an (initially empty) lib/pythonX.Y/site-packages subdirectory 
(on Windows, this is Lib\site-packages). If an existing directory is specified, it will be re-used.

*/

### Validate venv
python3 -m venv tutorial-env
ls -ltr
rm -rf tutorial-env


###Update variables  in the .profile file.
export PYSPARK_PYTHON=python3


###############################
####    JAVA Installation #####
###############################


### Install Java JDK
sudo apt-get install openjdk-8-jdk

### Validate Java
java -version
javac -version

###############################
####    localhost setup #####
###############################


### Check if SSH is installed
ssh

### Generate the provate and public key using ssh-keygen
ssh-keygen

### Copy the content of public key to authorized key file.
cat ~/.ssh/id_rsa.pub >> ~/.ssh/authorized_keys

### Test ssh localhost
ssh localhost

###Validate by exit
exit

## if above SSH server is not available , try using openssh-server
sudo apt remove openssh-server
sudo apt install openssh-server
sudo service ssh start




##############################################################
#### Downloading and setup all required softwares ############
#### Hadoop, HIVE , SPARK,              ######################
##############################################################


#### Download hadoop tar (Latest as of today).
wget https://mirrors.gigenet.com/apache/hadoop/common/hadoop-3.3.1/hadoop-3.3.1.tar.gz

### Download Hive.
wget https://mirrors.ocf.berkeley.edu/apache/hive/hive-3.1.2/apache-hive-3.1.2-bin.tar.gz


### Spark Website to Download. 
https://downloads.apache.org/spark/

### Download Spark3
wget https://ftp.wayne.edu/apache/spark/spark-3.2.1/spark-3.2.1-bin-hadoop3.2.tgz




### Untar the downloaded File
tar xfz hadoop-3.3.1.tar.gz
tar xzf apache-hive-3.1.2-bin.tar.gz
tar xzf spark-3.2.1-bin-hadoop3.2.tgz

#Archive all the softwares downloaded.

mkdir softwares

mv hadoop-3.3.1.tar.gz softwares
mv apache-hive-3.1.2-bin.tar.gz softwares
mv spark-3.2.1-bin-hadoop3.2.tgz softwares



### Set up folder structure.
sudo mv -f hadoop-3.3.1 /opt
sudo mv -f apache-hive-3.1.2-bin /opt
sudo mv -f spark-3.2.1-bin-hadoop3.2 /opt

### set up soft links
sudo ln -s /opt/hadoop-3.3.1 /opt/hadoop
sudo ln -s /opt/apache-hive-3.1.2-bin /opt/hive
sudo ln -s spark-3.2.1-bin-hadoop3.2 /opt/spark3



### Change the ownership to User.
sudo chown ${USER}:${USER} -R /opt/hadoop-3.3.1

### Update variables  in the .profile file.
export HADOOP_HOME=/opt/hadoop
export PATH=$PATH:$HADOOP_HOME/bin:$HADOOP_HOME/sbin 
export JAVA_HOME=/usr/lib/jvm/java-1.8.0-openjdk-amd64

export HIVE_HOME=/opt/hive
export PATH=$PATH:${HIVE_HOME}/bin
export PATH=$PATH:/opt/spark3/bin

### Source the .profile  
source .profile

### Validate if the changes are reflected.
echo $JAVA_HOME
echo $HADOOP_HOME
echo $PATH



########## HADOOP SETUP ################
# dG -- Clears all the data in the file
### core-site.xml : Informs Hadoop where NameNode runs in the cluster. 
vim /opt/hadoop/etc/hadoop/core-site.xml
<configuration>
    <property>
        <name>fs.defaultFS</name>
        <value>hdfs://localhost:9000</value>
    </property>
</configuration>


### hdfs-site.xml : Contains the configuration settings for HDFS daemons like the NameNode, the Secondary NameNode, and the DataNodes. 
vim /opt/hadoop/etc/hadoop/hdfs-site.xml
<configuration>
    <property>
        <name>dfs.namenode.name.dir</name>
        <value>/opt/hadoop/dfs/name</value>
    </property>
    <property>
        <name>dfs.namenode.checkpoint.dir</name>
        <value>/opt/hadoop/dfs/namesecondary</value>
    </property>
    <property>
        <name>dfs.datanode.data.dir</name>
        <value>/opt/hadoop/dfs/data</value>
    </property>
    <property>
        <name>dfs.replication</name>
        <value>1</value>
    </property>
	<property>
    <name>dfs.blocksize</name>
    <value>134217728</value>
  </property>
</configuration>


### hadoop-env.sh : Contains environment variable settings used by Hadoop.
vim /opt/hadoop/etc/hadoop/hadoop-env.sh 
export JAVA_HOME=/usr/lib/jvm/java-1.8.0-openjdk-amd64
export HADOOP_OS_TYPE=${HADOOP_OS_TYPE:-$(uname -s)}


### Format Namenode
hdfs namenode -format
ls -ltr /opt/hadoop/dfs/


###Start HDFS components.
start-dfs.sh


#### Validate the HDFS Services are running
jps

### Test HDFS Commands.
hadoop fs -ls /
hadoop fs -mkdir -p /user/${USER}
hadoop fs -ls /user/${USER}
hadoop fs -copyFromLocal test.txt /user/${USER}


### yarn-site.xml :  This file contains the yarn configuration options.
vim /opt/hadoop/etc/hadoop/yarn-site.xml

<configuration>
    <property>
        <name>yarn.nodemanager.aux-services</name>
        <value>mapreduce_shuffle</value>
    </property>
    <property>
        <name>yarn.nodemanager.env-whitelist</name>
        <value>HADOOP_COMMON_HOME,HADOOP_HDFS_HOME,JAVA_HOME,HADOOP_CONF_DIR,CLASSPATH_PREPEND_DISTCACHE,HADOOP_YARN_HOME,HADOOP_MAPRED_HOME</value>
    </property>
</configuration>

### mapred-site.xml : MapReduce configuration options are stored in this file.
vim /opt/hadoop/etc/hadoop/mapred-site.xml
<configuration>
    <property>
        <name>mapreduce.framework.name</name>
        <value>yarn</value>
    </property>
    <property>
        <name>mapreduce.application.classpath</name>
        <value>$HADOOP_MAPRED_HOME/share/hadoop/mapreduce/*:$HADOOP_MAPRED_HOME/share/hadoop/mapreduce/lib/*</value>
    </property>
</configuration>


### Start YARN Components.
start-yarn.sh

### Validate YARN Services are running.
jps


### All these start and stop services are available in :
/opt/hadoop/sbin

### Follow this order to stop the services.
1. stop-yarn.sh
2. stop-dfs.sh
3. Stop the Instance

### Follow this order to start the services.
1. start the Instance
2. start-dfs.sh
3. start-yarn.sh

##### POSTGRES SQL SETUP ######
### install postgres
sudo apt-get install postgresql
### set password to postgres
sudo passwd postgres
###To start the service, type 
sudo service postgresql start
###To conntect to postgres, type 
sudo -u postgres psql
\q
### Switch users to postgres by typing 
su - postgres
### psql

### create user hive in postgres window
$ sudo -u postgres createuser <username>
### create database metastore in pg window
sudo -u postgres createdb <dbname>
Giving the user a password

$ sudo -u postgres psql
psql=# alter user <username> with encrypted password '<password>';
Granting privileges on database

psql=# grant all privileges on database <dbname> to <username> ; 


###########   Hive Set up #####
###hive-site.xml : Global Configuration File for Hive
vim /opt/hive/conf/hive-site.xml
<configuration>
  <property>
    <name>javax.jdo.option.ConnectionURL</name>
    <value>jdbc:postgresql://localhost:5432/metastore</value>
    <description>JDBC Driver Connection for PostgrSQL</description>
  </property>
 
  <property>
    <name>javax.jdo.option.ConnectionDriverName</name>
    <value>org.postgresql.Driver</value>
    <description>PostgreSQL metastore driver class name</description>
  </property>
 
  <property>
    <name>javax.jdo.option.ConnectionUserName</name>
    <value>hive</value>
    <description>Database User Name</description>
  </property>
 
  <property>
    <name>javax.jdo.option.ConnectionPassword</name>
    <value>sibaram12</value>
    <description>Database User Password</description>
  </property>
</configuration>

### Update  /opt/hive/conf/hive-site.xml with below property.
vim /opt/hive/conf/hive-site.xml 
  <property>
    <name>hive.metastore.schema.verification</name>
    <value>false</value>
  </property>
 
### Remove the conflicting Guava Files if present.
rm /opt/hive/lib/guava-19.0.jar
cp /opt/hadoop/share/hadoop/hdfs/lib/guava-27.0-jre.jar /opt/hive/lib/

### Download a postgresql jar file and copy it to /opt/hive/lib/
wget https://jdbc.postgresql.org/download/postgresql-42.2.24.jar
sudo mv postgresql-42.2.24.jar /opt/hive/lib/postgresql-42.2.24.jar

### Initialize Hive Metastore
schematool -dbType postgres -initSchema

### Validate Metadata Tables
su -u postgres psql
\c metastore
\d
\q

################ SPARK SETUP ##################
## spark-env.sh
vim /opt/spark3/conf/spark-env.sh
export HADOOP_HOME="/opt/hadoop"
export HADOOP_CONF_DIR="/opt/hadoop/etc/hadoop"



#3.x
Update /opt/spark3/conf/spark-defaults.conf with below properties.
vim /opt/spark3/conf/spark-defaults.conf 
spark.driver.extraJavaOptions     -Dderby.system.home=/tmp/derby/
spark.sql.repl.eagerEval.enabled  true
spark.master                      yarn
spark.eventLog.enabled            true
spark.eventLog.dir                hdfs:///spark3-logs
spark.history.provider            org.apache.spark.deploy.history.FsHistoryProvider
spark.history.fs.logDirectory     hdfs:///spark3-logs
spark.history.fs.update.interval  10s
spark.history.ui.port             18080
spark.yarn.historyServer.address  localhost:18080
spark.yarn.jars                   hdfs:///spark3-jars/*.jar



#3.x
hdfs dfs -mkdir /spark3-jars
hdfs dfs -mkdir /spark3-logs

#3.x
hdfs dfs -put /opt/spark3/jars/* /spark3-jars 

#3.x
sudo ln -s /opt/hive/conf/hive-site.xml /opt/spark3/conf/

#3.x
sudo wget https://jdbc.postgresql.org/download/postgresql-42.2.19.jar \
    -O /opt/spark3/jars/postgresql-42.2.19.jar


### Validate Spark using Python 

#3.x
/opt/spark3/bin/pyspark --master yarn --conf spark.ui.port=0
spark.sql('SHOW databases').show()
spark.sql('USE test')
spark.sql('SELECT * FROM spark').show() 
exit()

### All the commands(spark-shell,pyspark,spark-submit) are avaiable in:
/opt/spark2/bin
/opt/spark3/bin


### Distringuish these commands in spark2 and spark3
#2.x
mv /opt/spark2/bin/pyspark /opt/spark2/bin/pyspark2
#3.x
mv /opt/spark3/bin/pyspark /opt/spark3/bin/pyspark3

### Validate the commands
#3.x
pyspark3 --master yarn






