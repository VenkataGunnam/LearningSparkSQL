ord = spark.read.load('file:///home/gunnamvsr/dataFiles/orders/part-00000.csv',sep=',',format='csv',\
schema=('order_id int,order_date timestamp,order_customer_id int,order_status string'))
  
ord.show()
# order is taked backup into ord_bkp 
ord_bkp = ord
  
data = [('Robert',35,32,31),('Robert',35,32,33),('Ram',30,25,30),('Ram',30,25,30)]
schema = ('name string, score1 int, score2 int, score3 int')
  
emp = spark.createDataFrame(data=data, schema = schema)
emp.show()

# ######################################################################################################################
#################################### S E L E C T   ########### D R O P - COL ########## R E N A M E ####################
############# DELETE DUPLICATES  ############ F UNCTION S ############### STACK ############## WITH COLUMN  ###########
#########################################################################################################################

ord.select(ord.order_id,'order_id',"order_id",(order_id+10).alias('orderadd'))


from pyspark.sql.functions import lower
ord.select(lower(ord.order_status),'order_status',lower("order_status"))
  
# " https://spark.apache.org/docs/latest/api/python/_modules/pyspark/sql/functions.html "  
# " all the functions are available in the above link   pyspark.sql.functions "
# " import all functions by using from pyspark.sql import functoins as f "

# "selectExpr" is used to query sql functions which are not available in the spark

ord.select(f.substring(ord.order_date,1,4).alias('orders')).show()
or
ord.selectExpr("substring(order_date,1,4) as orders").show()  # using selectExpr uses sql functions
ord.selectExpr("substr(order_date,1,4) as orders").show()  # using selectExpr uses sql functions

ord.selectExpr("lower(order_status), upper(order_status),nvl2(order_status,'CLOSED','ACTIVE')").show()

ord.selectExpr("lower(order_status) as los","upper(order_status) as uos ","nvl2(order_status,'CLOSED','COMPLETE') as stat")


# empty data frame
df=spark.range(1)
type(df)
df.selectExpr("stack(3, 1,2,3,4,5,6,7,8,9,10)").show()  # stack(n, pos1. .. .. ... .posx) dispalys n rows by splitting positions

>>> df.selectExpr("stack(2,1,2,3,4,5,6,7,8)")
+----+----+----+----+
|col0|col1|col2|col3|
+----+----+----+----+
|   1|   2|   3|   4|
|   5|   6|   7|   8|
+----+----+----+----+

# <==================== WithColumn(colName, col)  ==========================================>

# col_Name = alias name , new column will be created with the transformation applied
# if same name is given as alias name then new column wont be created, it applies transformation on the same column
# alias name same as column name is like update on the column
# alias name different as column is like new column with transformation applied on the column
from pyspark.sql import functions as f
ord.withColumn('order_created',f.substring(ord.order_date,1,4))
# order created is a new column with the tranformation applied on the order date
+--------+-------------------+-----------------+---------------+-------------+
|order_id|         order_date|order_customer_id|   order_status|order_created|
+--------+-------------------+-----------------+---------------+-------------+
|       1|2013-07-25 00:00:00|            11599|         CLOSED|         2013|
|       2|2013-07-25 00:00:00|              256|PENDING_PAYMENT|         2013|
|       3|2013-07-25 00:00:00|            12111|       COMPLETE|         2013|

ord.withColumn('order_date',f.substring(ord.order_date,1,4)).show()  #temporary
ord = ord.withColumn('order_date',f.substring(ord.order_date,1,4)).show()  #permanent
# order date is updated with the transformation
+--------+----------+-----------------+---------------+
|order_id|order_date|order_customer_id|   order_status|
+--------+----------+-----------------+---------------+
|       1|      2013|            11599|         CLOSED|
|       2|      2013|              256|PENDING_PAYMENT|
|       3|      2013|            12111|       COMPLETE|

# orders are initinalising back to original changes
ord = ord_bkp


# <===========================withColumnRenamed(existingcol, newcol) ===================================>
# this function is used to rename the column
ord.withColumnRenamed('order_date','order_created') #temporary changes 
ord = ord.withColumnRenamed('order_date','order_created') #permanent changes 


# <=========================drop(*cols) =====================================================>

ord.drop('order_id','order_date').show()  #temporary
ord_dropped = ord.drop('order_id','order_date').show()
ord_dropped.show() # ord_dropped has two columns dropped from order table


# <=========================dropDuplcates(*cols) =====================================================>

>>> emp.show()
+------+------+------+------+
|  name|score1|score2|score3|
+------+------+------+------+
|Robert|    35|    32|    31|
|Robert|    35|    32|    33|
|   Ram|    30|    25|    30|
|   Ram|    30|    25|    30|
+------+------+------+------+

>>> emp.dropDuplicates()  # duplicate records dropped on entire reocrds 
+------+------+------+------+
|  name|score1|score2|score3|
+------+------+------+------+
|Robert|    35|    32|    31|
|Robert|    35|    32|    33|
|   Ram|    30|    25|    30|
+------+------+------+------+


>>> emp.dropDuplicates(['name','score1'])  # dropped mentioned columns duplicate, schema should be given in the list here
+------+------+------+------+
|  name|score1|score2|score3|
+------+------+------+------+
|   Ram|    30|    25|    30|
|Robert|    35|    32|    31|
+------+------+------+------+


# ######################################################################################################################
#################################### F I L T E R   #####################################################################
############################################################### W H E R E ##############################################
########################################################################################################################
# filter(condition): (Its alias ‘where’)
# ✓ Filter rows using a given condition.
# ✓ use '&' for 'and‘. '|' for 'or‘. (boolean expressions) 
# ✓ Use column function isin() for multiple search. 
# ✓  Or use IN Operator for SQL Style syntax. 

ord.where(ord.order_id > 10) # not able to use 'order_id', "order_id" while comparing here

ord.where((ord.order_id > 10) & (ord.order_id < 30))
ord.where((ord.order_id > 10) | (ord.order_id < 30))

ord.where(((ord.order_id > 10) & (ord.order_id < 30))| (ord.order_status.isin ('CLOSED','COMPLETE')))
ord.where(ord.order_status.isin ('CLOSED','COMPLETE'))

#SQL STyle
ord.where("order_status in ('CLOSED','COMPLETE')")
ord.where("order_id > 10")
ord.where("(order_id > 10) and (order_id < 20)")
ord.selectExpr("concat(lower(order_status),' - ', upper(order_status) ) as uos" )
ord.selectExpr("lower(order_status) ||' - ' || upper(order_status) as uos ")


# ######################################################################################################################
#################################### SORTING  / orderBy  #####################################################################  
######################################################################### ASC()  DESC() ################################ 
########################################################################################################################

#Sorting is the costliest operation, try to avoid if required
# sort() or orderBy(): 
# ✓ Sort specific column(s).
# ord.sort(ord.order_date.desc(),ord.order_status.asc()).show()
# ord.sort(ord.order_date,ord.order_status,ascending=(0,1)).show() #1 Ascending, 0 Descending
# • sortWithinPartitions:
# ✓ At time, we may not want sort globally, but with in a group. In that case we can use sortWithinPartitions.
# df.sortWithinPartitions(df.col1.asc(),df.col2.asc()).show()

# to get number of partitions use   df.rdd.getNumPartitions()
# use glom function to see how data is distributed in those partitions df.rdd.glom().collect

data=(('a',1,3),('d',4,2),('c',3,5),('b',2,7),('e',2,1))
df = spark.createDataFrame(data=data,schema='col1 string,col2 int,col3 int')

df.sort(df.col2.asc())
df.sort(df.col2.asc(),df.col3.desc()) 

df.sort(df.col2,df.col3,ascending=[1,0])  # 1 = ascending, 0 descending , this ishould be in list 

df.sort('col2','col3',ascending=[1,0])

 # by default using orderBy or sort considers Ascending order
 df.orderBy(df.col2.asc(),df.col3.asc())
 df.orderBy(df.col2,df.col3)
 
 
df.rdd.getNumPartitions()
df.rdd.glom().collect()
[
[Row(col1='a', col2=1, col3=3), Row(col1='d', col2=4, col3=2)], 
[Row(col1='c', col2=3, col3=5), Row(col1='b', col2=2, col3=7), Row(col1='e', col2=2, col3=1)]
]
>>> df.sortWithinPartitions(df.col1.asc())
+----+----+----+
|col1|col2|col3|
+----+----+----+
|   a|   1|   3|
|   d|   4|   2|
|   b|   2|   7|
|   c|   3|   5|
|   e|   2|   1|
+----+----+----+





# ######################################################################################################################
#################################### SET OPERATOR #####################################################################  
######### UNION/UNIONALL ###### unionByName() ####################### intersec/intersecAll ########## exceptAll ####### 
########################################################################################################################
#• union() and unionAll(): 
#✓ Same and contains duplicate values. 
#✓ Use distinct after union or unionAll to remove duplicates .
#• unionByName():
#✓ The difference between this function and :func:`union` is that this function
#resolves columns by name (not by position)
#df1 = spark.createDataFrame(data=(('a',1),('b',2)),schema=('col1 string,col2 int'))
#df2 = spark.createDataFrame(data=((2,'b'),(3,'c')),schema=('col2 int,col1 string'))
#df1.union(df2).show()
#df1.unionByName(df2).show()
#• intersect(): Containing rows in both DataFrames. Removed duplicates.
#• intersectAll(): Same as intersect. But retains the duplicates.
#df1 = spark.createDataFrame(data=(('a',1),('a',1),('b',2)),schema=('col1 string,col2 int')) 
#df2 = spark.createDataFrame(data=(('a',1),('a',1),('c',2)),schema=('col1 string,col2 int'))
#df1.intersect(df2).show() 
#df1.intersectAll(df2).show()
#• exceptAll(): Rows present in one DataFrame but not in another.
#df1.exceptAll(df2).show()
#Set Operator APIs
#

 
 
 
>>> df1 = spark.createDataFrame(data=(('a',1),('b',2)),schema=('col1 string,col2 int'))
>>> df2 = spark.createDataFrame(data=((2,'b'),(3,'c')),schema=('col2 int,col1 string'))
>>>
>>>
>>> df1.union(df2)
+----+----+
|col1|col2|
+----+----+
|   a|   1|
|   b|   2|
|   2|   b|
|   3|   c|
+----+----+

>>> df1.unionByName(df2).show()
+----+----+
|col1|col2|
+----+----+
|   a|   1|
|   b|   2|
|   b|   2|
|   c|   3|
+----+----+


>>> df1.unionByName(df2).distinct()
+----+----+
|col1|col2|
+----+----+
|   a|   1|
|   b|   2|
|   c|   3|
+----+----+

 df3 = spark.range(1,10)
>>> type(df3)
<class 'pyspark.sql.dataframe.DataFrame'>
>>>
>>>
>>> df4 = spark.range(5,15)
>>> df3.exceptAll(df4)
+---+
| id|
+---+
|  1|
|  3|
|  2|
|  4|
+---+



# ######################################################################################################################
#################################### JOINS####### INNER ############ RIGHTOUTER ####### LEFTOUTER #### CROSS ###########
###### CROSSJOIN ##### LEFTSEMI  ########### RIGHTSEMI ########## LEFTANTI ######### RIGHTANTI ######### FULLOUTER #####
########################################################################################################################






data = [(1,'Robert',25,2000,'ELR'),(2,'Ravi',20,3000,'VJR'),(3,'Ramu',28,2500,'HYD'),(4,'Kumar',32,4000,'BZA'),(4,'Venkat',30,3200,'HYD'),(5,'Krishna',29,3000,'ELR')]
data2 = [(1,'Robert',25,2000,'ELR'),(1,'Ravi',20,3000,'VJR'),(3,'Ramu',28,2500,'HYD'),(4,'Kumar',32,4000,'BZA'),(4,'Venkat',30,3200,'HYD')]


df1=spark.createDataFrame(data=data,schema=schema)

df2=spark.createDataFrame(data=data2,schema=schema)


 df1.show()
+---+-------+---+----+----+
|Sno|   name|age| sal|area|
+---+-------+---+----+----+
|  1| Robert| 25|2000| ELR|
|  2|   Ravi| 20|3000| VJR|
|  3|   Ramu| 28|2500| HYD|
|  4|  Kumar| 32|4000| BZA|
|  4| Venkat| 30|3200| HYD|
|  5|Krishna| 29|3000| ELR|
+---+-------+---+----+----+

df2.show()
+---+------+---+----+----+
|Sno|  name|age| sal|area|
+---+------+---+----+----+
|  1|Robert| 25|2000| ELR|
|  1|  Ravi| 20|3000| VJR|
|  3|  Ramu| 28|2500| HYD|
|  4| Kumar| 32|4000| BZA|
|  4|Venkat| 30|3200| HYD|
+---+------+---+----+----+


# cross join ( 6 * 5 = 30 )
 df1.join(df2)
+---+-------+---+----+----+---+------+---+----+----+
|Sno|   name|age| sal|area|Sno|  name|age| sal|area|
+---+-------+---+----+----+---+------+---+----+----+
|  1| Robert| 25|2000| ELR|  1|Robert| 25|2000| ELR|
|  1| Robert| 25|2000| ELR|  1|  Ravi| 20|3000| VJR|
|  2|   Ravi| 20|3000| VJR|  1|Robert| 25|2000| ELR|
|  2|   Ravi| 20|3000| VJR|  1|  Ravi| 20|3000| VJR|
|  3|   Ramu| 28|2500| HYD|  1|Robert| 25|2000| ELR|
|  3|   Ramu| 28|2500| HYD|  1|  Ravi| 20|3000| VJR|
|  1| Robert| 25|2000| ELR|  3|  Ramu| 28|2500| HYD|
|  1| Robert| 25|2000| ELR|  4| Kumar| 32|4000| BZA|


# inner join on Sno
 df1.join(df2, df1.Sno==df2.Sno,'inner')
+---+------+---+----+----+---+------+---+----+----+
|Sno|  name|age| sal|area|Sno|  name|age| sal|area|
+---+------+---+----+----+---+------+---+----+----+
|  1|Robert| 25|2000| ELR|  1|Robert| 25|2000| ELR|
|  1|Robert| 25|2000| ELR|  1|  Ravi| 20|3000| VJR|
|  3|  Ramu| 28|2500| HYD|  3|  Ramu| 28|2500| HYD|
|  4| Kumar| 32|4000| BZA|  4| Kumar| 32|4000| BZA|
|  4| Kumar| 32|4000| BZA|  4|Venkat| 30|3200| HYD|
|  4|Venkat| 30|3200| HYD|  4| Kumar| 32|4000| BZA|
|  4|Venkat| 30|3200| HYD|  4|Venkat| 30|3200| HYD|
+---+------+---+----+----+---+------+---+----+----+


# not mentioned type of join to be used , default considered inner join
 df1.join(df2, df1.Sno==df2.Sno)
+---+------+---+----+----+---+------+---+----+----+
|Sno|  name|age| sal|area|Sno|  name|age| sal|area|
+---+------+---+----+----+---+------+---+----+----+
|  1|Robert| 25|2000| ELR|  1|Robert| 25|2000| ELR|
|  1|Robert| 25|2000| ELR|  1|  Ravi| 20|3000| VJR|
|  3|  Ramu| 28|2500| HYD|  3|  Ramu| 28|2500| HYD|
|  4| Kumar| 32|4000| BZA|  4| Kumar| 32|4000| BZA|
|  4| Kumar| 32|4000| BZA|  4|Venkat| 30|3200| HYD|
|  4|Venkat| 30|3200| HYD|  4| Kumar| 32|4000| BZA|
|  4|Venkat| 30|3200| HYD|  4|Venkat| 30|3200| HYD|
+---+------+---+----+----+---+------+---+----+----+

>>> df1.join(df2, df1.Sno==df2.Sno,'leftouter')
+---+-------+---+----+----+----+------+----+----+----+
|Sno|   name|age| sal|area| Sno|  name| age| sal|area|
+---+-------+---+----+----+----+------+----+----+----+
|  1| Robert| 25|2000| ELR|   1|Robert|  25|2000| ELR|
|  1| Robert| 25|2000| ELR|   1|  Ravi|  20|3000| VJR|
|  2|   Ravi| 20|3000| VJR|null|  null|null|null|null|
|  3|   Ramu| 28|2500| HYD|   3|  Ramu|  28|2500| HYD|
|  4|  Kumar| 32|4000| BZA|   4| Kumar|  32|4000| BZA|
|  4|  Kumar| 32|4000| BZA|   4|Venkat|  30|3200| HYD|
|  4| Venkat| 30|3200| HYD|   4| Kumar|  32|4000| BZA|
|  4| Venkat| 30|3200| HYD|   4|Venkat|  30|3200| HYD|
|  5|Krishna| 29|3000| ELR|null|  null|null|null|null|
+---+-------+---+----+----+----+------+----+----+----+


df1.join(df2, df1.Sno==df2.Sno,'leftouter').select(df1.Sno.alias('df1sno') , df1.name.alias('df1name'),
 df2.Sno.alias('df2Sno'), df2.name.alias('df2name'))
+------+-------+------+-------+
|df1sno|df1name|df2Sno|df2name|
+------+-------+------+-------+
|     1| Robert|     1| Robert|
|     1| Robert|     1|   Ravi|
|     2|   Ravi|  null|   null|
|     3|   Ramu|     3|   Ramu|
|     4|  Kumar|     4|  Kumar|
|     4|  Kumar|     4| Venkat|
|     4| Venkat|     4|  Kumar|
|     4| Venkat|     4| Venkat|
|     5|Krishna|  null|   null|
+------+-------+------+-------+


# left semi populates only matching records from the left table
# it does not multiply with the right table for matched records like inner join
# it just displays the left table which has matched records in the right table
 df1.join(df2, df1.Sno==df2.Sno,'leftsemi')
+---+------+---+----+----+
|Sno|  name|age| sal|area|
+---+------+---+----+----+
|  1|Robert| 25|2000| ELR|
|  3|  Ramu| 28|2500| HYD|
|  4| Kumar| 32|4000| BZA|
|  4|Venkat| 30|3200| HYD|
+---+------+---+----+----+

# left anti populates records from the left table which are unmatched
# only records from the left table

df1.join(df2, df1.Sno==df2.Sno,'leftanti')
+---+-------+---+----+----+
|Sno|   name|age| sal|area|
+---+-------+---+----+----+
|  2|   Ravi| 20|3000| VJR|
|  5|Krishna| 29|3000| ELR|
+---+-------+---+----+----+

# select, where, join in single query
df1.join(df2, df1.Sno==df2.Sno,'inner')
.where(df1.sal>2500)
.select(df1.Sno.alias('df1sno') , df1.name.alias('df1name'), df2.Sno.alias('df2Sno'), 
		df2.name.alias('df2name'),df1.sal.alias('df1sal'))
+------+-------+------+-------+------+
|df1sno|df1name|df2Sno|df2name|df1sal|
+------+-------+------+-------+------+
|     4|  Kumar|     4|  Kumar|  4000|
|     4|  Kumar|     4| Venkat|  4000|
|     4| Venkat|     4|  Kumar|  3200|
|     4| Venkat|     4| Venkat|  3200|
+------+-------+------+-------+------+


>>>
>>> df3.show()
+---+-------+---+----+----+
|Sno|   name|age| sal|area|
+---+-------+---+----+----+
|  1| Robert| 25|2000| ELR|
|  2|   Ravi| 20|3000| VJR|
|  3|   Ramu| 28|2500| HYD|
|  4|  Kumar| 32|4000| BZA|
|  4| Venkat| 30|3200| HYD|
|  5|Krishna| 29|3000| ELR|
+---+-------+---+----+----+

#multi joins 
>>> df1.join(df2, (df1.Sno==df2.Sno) & (df1.sal == df2.sal))
+---+------+---+----+----+---+------+---+----+----+
|Sno|  name|age| sal|area|Sno|  name|age| sal|area|
+---+------+---+----+----+---+------+---+----+----+
|  1|Robert| 25|2000| ELR|  1|Robert| 25|2000| ELR|
|  3|  Ramu| 28|2500| HYD|  3|  Ramu| 28|2500| HYD|
|  4|Venkat| 30|3200| HYD|  4|Venkat| 30|3200| HYD|
|  4| Kumar| 32|4000| BZA|  4| Kumar| 32|4000| BZA|
+---+------+---+----+----+---+------+---+----+----+

# Multi table joins
>>> df1.join(df2, (df1.Sno==df2.Sno) & (df1.sal == df2.sal))
		.join(df3, (df3.Sno==df1.Sno)&(df3.sal==df1.sal))
+---+------+---+----+----+---+------+---+----+----+---+------+---+----+----+
|Sno|  name|age| sal|area|Sno|  name|age| sal|area|Sno|  name|age| sal|area|
+---+------+---+----+----+---+------+---+----+----+---+------+---+----+----+
|  3|  Ramu| 28|2500| HYD|  3|  Ramu| 28|2500| HYD|  3|  Ramu| 28|2500| HYD|
|  1|Robert| 25|2000| ELR|  1|Robert| 25|2000| ELR|  1|Robert| 25|2000| ELR|
|  4| Kumar| 32|4000| BZA|  4| Kumar| 32|4000| BZA|  4| Kumar| 32|4000| BZA|
|  4|Venkat| 30|3200| HYD|  4|Venkat| 30|3200| HYD|  4|Venkat| 30|3200| HYD|
+---+------+---+----+----+---+------+---+----+----+---+------+---+----+----+




# ######################################################################################################################
#################################### AGGREGATIONS ######################################################################
########################################################################################################################

# Summary -- count, max, mean, min ,sum
# avg, max, min, sum, sumDistinct, count, countDistinct, first, last, collect_set, collect_list
# skewness, variance, stddev

>>> ord.summary()
+-------+------------------+-----------------+---------------+
|summary|          order_id|order_customer_id|   order_status|
+-------+------------------+-----------------+---------------+
|  count|             68883|            68883|          68883|
|   mean|           34442.0|6216.571098819738|           null|
| stddev|19884.953633337947|3586.205241263963|           null|
|    min|                 1|                1|       CANCELED|
|    25%|             17214|             3121|           null|
|    50%|             34438|             6198|           null|
|    75%|             51657|             9325|           null|
|    max|             68883|            12435|SUSPECTED_FRAUD|
+-------+------------------+-----------------+---------------+


>>> ord.select("order_id","order_customer_id").summary("count")
+-------+--------+-----------------+
|summary|order_id|order_customer_id|
+-------+--------+-----------------+
|  count|   68883|            68883|
+-------+--------+-----------------+

from pyspark.sql.functions import *
>>> ord.select(count(ord.order_id))
+---------------+
|count(order_id)|
+---------------+
|          68883|
+---------------+


>>> ord.select(round(avg(ord.order_id)).alias('averg'), max(ord.order_id).alias('maximum'), min(ord.order_id).alias('minimum'))
+-------+-------+-------+
|  averg|maximum|minimum|
+-------+-------+-------+
|34442.0|  68883|      1|
+-------+-------+-------+



>>> ord.select(round(avg(ord.order_id)).alias('averg'), max(ord.order_id).alias('maximum'), min(ord.order_id).alias('minimum'), sum_distinct(ord.order_id).a
lias('distinctsum'), sum(ord.order_id).alias('sumid'),count(ord.order_id).alias('idcount'),countDistinct(ord.order_id).alias('idcountdistinct'))
+-------+-------+-------+-----------+----------+-------+---------------+
|  averg|maximum|minimum|distinctsum|     sumid|idcount|idcountdistinct|
+-------+-------+-------+-----------+----------+-------+---------------+
|34442.0|  68883|      1| 2372468286|2372468286|  68883|          68883|
+-------+-------+-------+-----------+----------+-------+---------------+


 ord.sort(ord.order_id.asc()).select(first(ord.order_id))
+---------------+
|first(order_id)|
+---------------+
|              1|
+---------------+

>>> ord.sort(ord.order_id.asc()).select(last(ord.order_id))
+--------------+
|last(order_id)|
+--------------+
|         68883|
+--------------+


>>> ord_new.select(collect_list(ord_new.order_status).alias('colectedlist'), 
					collect_set(ord_new.order_status).alias('collectedset')).show(100 , False)
+--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+------------------------------------------------------------------------+
|colectedlist                                                                                                                                                                                                                                                                                      |collectedset                                                            |
+--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+------------------------------------------------------------------------+
|[CLOSED, PENDING_PAYMENT, COMPLETE, CLOSED, COMPLETE, COMPLETE, COMPLETE, PROCESSING, PENDING_PAYMENT, PENDING_PAYMENT, PAYMENT_REVIEW, CLOSED, PENDING_PAYMENT, PROCESSING, COMPLETE, PENDING_PAYMENT, COMPLETE, CLOSED, PENDING_PAYMENT, PROCESSING, PENDING, COMPLETE, PENDING_PAYMENT, CLOSED]|[PAYMENT_REVIEW, PENDING, PROCESSING, PENDING_PAYMENT, CLOSED, COMPLETE]|
+--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+------------------------------------------------------------------------+



# Winodw Functions
# Window, WindowSpec
# partitionBy, orderBy, rangeBetween, rowsBetween
# for group of rows return single row
# > Ranking, Analytical and Aggregate
from pyspark.sql.window import *
from pyspark.sql import window
help(window)
					
data = \
(("James","Sales","NY",9000,34), \
("Alicia","Sales","NY",8600,56), \
("Robert","Sales","CA",8100,30), \
("John","Sales","AZ",8600,31), \
("Ross","Sales","AZ",8100,33), \
("Kathy","Sales","AZ",1000,39), \
("Lisa","Finance","CA",9000,24), \
("Deja","Finance","CA",9900,40), \
("Sugie","Finance","NY",8300,36), \
("Ram","Finance","NY",7900,53), \
("Satya","Finance","AZ",8200,53), \
("Kyle","Marketing","CA",8000,25), \
("Reid","Marketing","NY",9100,50) \
)
					
schema=("empname","dept","state","salary","age")

df= spark.createDataFrame ( data,schema =schema)
# Ranking functions, 
# row_number(),  rank(),  dense_rank() , percent_Rank(),  ntile(),  cume_dist()

spec = Window.partitionBy(df.dept).orderBy(df.dept.asc(),df.salary.desc())

 >>> df.select("dept","salary").withColumn("row_number_Rank",row_number().over(spec)
... ).withColumn("rank",rank().over(spec)
... ).withColumn("denseRank",dense_rank().over(spec)
... ).withColumn("PercentRrank",percent_rank().over(spec)
... ).withColumn("ntile",ntile(3).over(spec) # ntile excepts only single parameter, it gives numbers till provided value, by equating
... ).withColumn("cum_dist",cume_dist().over(spec)
... )

+---------+------+---------------+----+---------+------------+-----+-------------------+
|     dept|salary|row_number_Rank|rank|denseRank|PercentRrank|ntile|           cum_dist|
+---------+------+---------------+----+---------+------------+-----+-------------------+
|  Finance|  9900|              1|   1|        1|         0.0|    1|                0.2|
|  Finance|  9000|              2|   2|        2|        0.25|    1|                0.4|
|  Finance|  8300|              3|   3|        3|         0.5|    2|                0.6|
|  Finance|  8200|              4|   4|        4|        0.75|    2|                0.8|
|  Finance|  7900|              5|   5|        5|         1.0|    3|                1.0|
|Marketing|  9100|              1|   1|        1|         0.0|    1|                0.5|
|Marketing|  8000|              2|   2|        2|         1.0|    2|                1.0|
|    Sales|  9000|              1|   1|        1|         0.0|    1|0.16666666666666666|
|    Sales|  8600|              2|   2|        2|         0.2|    1|                0.5|
|    Sales|  8600|              3|   2|        2|         0.2|    2|                0.5|
|    Sales|  8100|              4|   4|        3|         0.6|    2| 0.8333333333333334|
|    Sales|  8100|              5|   4|        3|         0.6|    3| 0.8333333333333334|
|    Sales|  1000|              6|   6|        4|         1.0|    3|                1.0|
+---------+------+---------------+----+---------+------------+-----+-------------------+

# Analytical
#lag and lead lag(1,0), lead(1,0) -- lag by 1 and if null 0

>>> df.select("dept","salary").withColumn("row_number_Rank",row_number().over(spec)
... ).withColumn("lag_val",lag("salary").over(spec)
... ).withColumn("lead_Val",lead("salary",1,9999).over(spec)
... )
+---------+------+---------------+-------+--------+
|     dept|salary|row_number_Rank|lag_val|lead_Val|
+---------+------+---------------+-------+--------+
|  Finance|  9900|              1|   null|    9000|
|  Finance|  9000|              2|   9900|    8300|
|  Finance|  8300|              3|   9000|    8200|
|  Finance|  8200|              4|   8300|    7900|
|  Finance|  7900|              5|   8200|    9999|
|Marketing|  9100|              1|   null|    8000|
|Marketing|  8000|              2|   9100|    9999|
|    Sales|  9000|              1|   null|    8600|
|    Sales|  8600|              2|   9000|    8600|
|    Sales|  8600|              3|   8600|    8100|
|    Sales|  8100|              4|   8600|    8100|
|    Sales|  8100|              5|   8100|    1000|
|    Sales|  1000|              6|   8100|    9999|
+---------+------+---------------+-------+--------+


# Aggregate
# avg, sum, max ,min , count, first, last 
>>> df.select("dept","salary")\
... .withColumn("sum_sal",sum("salary").over(spec)
... ).withColumn("max_val",max("salary").over(spec)
... ).withColumn("min_val",min("salary").over(spec)
... ).withColumn("count",count("salary").over(spec)
... ).withColumn("first",first("salary").over(spec)
... ).withColumn("last",last("salary").over(spec)
... )
+---------+------+-------+-------+-------+-----+-----+----+
|     dept|salary|sum_sal|max_val|min_val|count|first|last|
+---------+------+-------+-------+-------+-----+-----+----+
|  Finance|  9900|   9900|   9900|   9900|    1| 9900|9900|
|  Finance|  9000|  18900|   9900|   9000|    2| 9900|9000|
|  Finance|  8300|  27200|   9900|   8300|    3| 9900|8300|
|  Finance|  8200|  35400|   9900|   8200|    4| 9900|8200|
|  Finance|  7900|  43300|   9900|   7900|    5| 9900|7900|
|Marketing|  9100|   9100|   9100|   9100|    1| 9100|9100|
|Marketing|  8000|  17100|   9100|   8000|    2| 9100|8000|
|    Sales|  9000|   9000|   9000|   9000|    1| 9000|9000|
|    Sales|  8600|  26200|   9000|   8600|    3| 9000|8600|
|    Sales|  8600|  26200|   9000|   8600|    3| 9000|8600|
|    Sales|  8100|  42400|   9000|   8100|    5| 9000|8100|
|    Sales|  8100|  42400|   9000|   8100|    5| 9000|8100|
|    Sales|  1000|  43400|   9000|   1000|    6| 9000|1000|
+---------+------+-------+-------+-------+-----+-----+----+


# rangeBetween, rowsBetween
# Takes two argument (start,end) to define frame boundaries
# Default : unboundedPreceding and unboundedFollowing.
# preceding takes first record , following record takes last record
# Range arguements applied on the range, where as rows arguement applid on the rows
# ``Window.currentRow``  considers current row
# eg. if there is 100 rows and range is given as (1,10)
# eg, if there is 100 rows and rows is given as (1,2) then only 2 rows are taken

from pyspark.sql import *
>>> from pyspark.sql import Window
>>>
>>> from pyspark.sql.functions import *
>>> spec=Window.partitionBy(df.dept).orderBy(df.salary.asc()).rangeBetween(Window.unboundedPreceding, Window.unboundedFo
llowing)
>>>
>>> df.select(df.dept,df.salary).withColumn("sum_salary",sum(df.salary).over(spec))
+---------+------+----------+
|     dept|salary|sum_salary|
+---------+------+----------+
|  Finance|  7900|     43300|
|  Finance|  8200|     43300|
|  Finance|  8300|     43300|
|  Finance|  9000|     43300|
|  Finance|  9900|     43300|
|Marketing|  8000|     17100|
|Marketing|  9100|     17100|
|    Sales|  1000|     43400|
|    Sales|  8100|     43400|
|    Sales|  8100|     43400|
|    Sales|  8600|     43400|
|    Sales|  8600|     43400|
|    Sales|  9000|     43400|
+---------+------+----------+


>>>
>>> spec1=Window.partitionBy(df.dept).orderBy(df.salary.asc()).rowsBetween(Window.unboundedPreceding, Window.unboundedFo
llowing)
>>>
>>>
>>> df.select(df.dept,df.salary).withColumn("sum_salary",sum(df.salary).over(spec1))
+---------+------+----------+
|     dept|salary|sum_salary|
+---------+------+----------+
|  Finance|  7900|     43300|
|  Finance|  8200|     43300|
|  Finance|  8300|     43300|
|  Finance|  9000|     43300|
|  Finance|  9900|     43300|
|Marketing|  8000|     17100|
|Marketing|  9100|     17100|
|    Sales|  1000|     43400|
|    Sales|  8100|     43400|
|    Sales|  8100|     43400|
|    Sales|  8600|     43400|
|    Sales|  8600|     43400|
|    Sales|  9000|     43400|
+---------+------+----------+

spec3=Window.partitionBy(df.dept).orderBy(df.salary.asc()).rowsBetween(Window.currentRow, Window.unboundedFollowing)
>>>
>>>
>>> df.select(df.dept,df.salary).withColumn("sum_salary",sum(df.salary).over(spec3))
+---------+------+----------+
|     dept|salary|sum_salary|
+---------+------+----------+
|  Finance|  7900|     43300|
|  Finance|  8200|     35400|
|  Finance|  8300|     27200|
|  Finance|  9000|     18900|
|  Finance|  9900|      9900|
|Marketing|  8000|     17100|
|Marketing|  9100|      9100|
|    Sales|  1000|     43400|
|    Sales|  8100|     42400|
|    Sales|  8100|     34300|
|    Sales|  8600|     26200|
|    Sales|  8600|     17600|
|    Sales|  9000|      9000|
+---------+------+----------+


>>> df.select(df.dept,df.salary).withColumn("sum_salary",sum(df.salary).over(spec2))
+---------+------+----------+
|     dept|salary|sum_salary|
+---------+------+----------+
|  Finance|  7900|     43300|
|  Finance|  8200|     35400|
|  Finance|  8300|     27200|
|  Finance|  9000|     18900|
|  Finance|  9900|      9900|
|Marketing|  8000|     17100|
|Marketing|  9100|      9100|
|    Sales|  1000|     43400|
|    Sales|  8100|     42400|
|    Sales|  8100|     42400|
|    Sales|  8600|     26200|
|    Sales|  8600|     26200|
|    Sales|  9000|      9000|
+---------+------+----------+


>>> spec4=Window.partitionBy(df.dept).orderBy(df.salary.asc()).rangeBetween(Window.currentRow, 250)
>>> df.select(df.dept,df.salary).withColumn("sum_salary",sum(df.salary).over(spec4))
+---------+------+----------+
|     dept|salary|sum_salary|
+---------+------+----------+
|  Finance|  7900|      7900|
|  Finance|  8200|     16500|
|  Finance|  8300|      8300|
|  Finance|  9000|      9000|
|  Finance|  9900|      9900|
|Marketing|  8000|      8000|
|Marketing|  9100|      9100|
|    Sales|  1000|      1000|
|    Sales|  8100|     16200|
|    Sales|  8100|     16200|
|    Sales|  8600|     17200|
|    Sales|  8600|     17200|
|    Sales|  9000|      9000|
+---------+------+----------+


>>> spec5=Window.partitionBy(df.dept).orderBy(df.salary.asc()).rowsBetween(Window.currentRow, 2)
>>> df.select(df.dept,df.salary).withColumn("sum_salary",sum(df.salary).over(spec5))
+---------+------+----------+
|     dept|salary|sum_salary|
+---------+------+----------+
|  Finance|  7900|     24400|
|  Finance|  8200|     25500|
|  Finance|  8300|     27200|
|  Finance|  9000|     18900|
|  Finance|  9900|      9900|
|Marketing|  8000|     17100|
|Marketing|  9100|      9100|
|    Sales|  1000|     17200|
|    Sales|  8100|     24800|
|    Sales|  8100|     25300|
|    Sales|  8600|     26200|
|    Sales|  8600|     17600|
|    Sales|  9000|      9000|
+---------+------+----------+

# Sampling
# sampe(replacement, fraction/percentage, seed( same data freeze) )
# sampleBy(self,col,fractions,seed)
>>> df.sample(False,0.7,True)
+-------+---------+-----+------+---+
|empname|     dept|state|salary|age|
+-------+---------+-----+------+---+
|  James|    Sales|   NY|  9000| 34|
| Alicia|    Sales|   NY|  8600| 56|
| Robert|    Sales|   CA|  8100| 30|
|   John|    Sales|   AZ|  8600| 31|
|   Lisa|  Finance|   CA|  9000| 24|
|   Deja|  Finance|   CA|  9900| 40|
|  Sugie|  Finance|   NY|  8300| 36|
|    Ram|  Finance|   NY|  7900| 53|
|   Kyle|Marketing|   CA|  8000| 25|
|   Reid|Marketing|   NY|  9100| 50|
+-------+---------+-----+------+---+

>>> df.sample(True,0.7,True)
+-------+---------+-----+------+---+
|empname|     dept|state|salary|age|
+-------+---------+-----+------+---+
|  James|    Sales|   NY|  9000| 34|
|  James|    Sales|   NY|  9000| 34|
| Alicia|    Sales|   NY|  8600| 56|
| Robert|    Sales|   CA|  8100| 30|
|   John|    Sales|   AZ|  8600| 31|
|   John|    Sales|   AZ|  8600| 31|
|  Kathy|    Sales|   AZ|  1000| 39|
|    Ram|  Finance|   NY|  7900| 53|
|   Kyle|Marketing|   CA|  8000| 25|
|   Kyle|Marketing|   CA|  8000| 25|
|   Kyle|Marketing|   CA|  8000| 25|
+-------+---------+-----+------+---+


# Other aggregate functions
# First(ignorenulls=false), last(ignorenulls=false), greatest, skewness, least , collect_list
>>> df.select(first(df.salary,False))
+-------------+
|first(salary)|
+-------------+
|         9000|
+-------------+


>>> df.select(collect_list(df.dept)).show(truncate=False)
+-------------------------------------------------------------------------------------------------------------+
|collect_list(dept)                                                                                           |
+-------------------------------------------------------------------------------------------------------------+
|[Sales, Sales, Sales, Sales, Sales, Sales, Finance, Finance, Finance, Finance, Finance, Marketing, Marketing]|
+-------------------------------------------------------------------------------------------------------------+