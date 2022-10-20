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
############# DELETE DUPLICATES  ############ F UNCTION S ############### STACK #################################
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

ord.selectExpr("lower(order_status) as los","upper(order_status) as uos ","nvl2(order_status,'CLOSED','COMPLETE') as stat").
show()


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
#########################################################################################################################