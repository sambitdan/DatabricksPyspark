# Databricks notebook source
# MAGIC %md
# MAGIC ### Data Reading

# COMMAND ----------

# checking file path using fs(file system) command

dbutils.fs.ls('/FileStore')

# COMMAND ----------

dbutils.fs.ls('/FileStore/tables')

# COMMAND ----------

df=spark.read.format('csv').option('inferSchema',True).option('header',True).load('/FileStore/tables/BigMart_Sales.csv')
# spark.read is dataframe reader api within pyspark
# by using .option() we are bringing schema, schema is basically information about the table like datatype of columns.
# by using 'inferSchema' spark will automatically decide the best fit schema for a column by looking into the few records.
#spark.read.table('table_name/table_path') to read tables

# COMMAND ----------

#instead of df.show() we can use df.display() to visualize in more proper way
df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC #### Data Reading For JSON

# COMMAND ----------

dbutils.fs.ls('/FileStore/tables')

# COMMAND ----------

df_json=spark.read.format('json').option('inferSchema',True)\
                            .option('header',True)\
                            .option('multiline',False)\
                            .load('/FileStore/tables/drivers.json')
# json can store data in single line format or multiline in our data it is singleline in single one record is store in one line thenanother line store another record

# COMMAND ----------

df_json.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Schema Definition

# COMMAND ----------

df.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC #### DDL Schema

# COMMAND ----------

my_ddl_schema='''
                  Item_Identifier string,
                  Item_Weight string,
                  Item_Fat_Content string,
                  Item_Visibility double,
                  Item_Type string,
                  Item_MRP double,
                  Outlet_Identifier string,
                  Outlet_Establishment_Year integer,
                  Outlet_Size string,
                  Outlet_Location_Type string,
                  Outlet_Type string,
                  Item_Outlet_Sales double
                '''

# COMMAND ----------

df = spark.read.format('csv').schema(my_ddl_schema).option('header',True).load('/FileStore/tables/BigMart_Sales.csv')

# COMMAND ----------

df.printSchema()

# COMMAND ----------

df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC #### StructType() Schema

# COMMAND ----------

# before using StructType()/StructField() we need to import some libraries
from pyspark.sql.types import *  # for importing type
from pyspark.sql.functions import * # for importing functions like structfield

# COMMAND ----------

df.printSchema()

# COMMAND ----------

my_strct_schema=StructType([
                              StructField('Item_Identifier',StringType(),True), #the last true tells us nullable or not
                              StructField('Item_Weight',DoubleType(),True),
                              StructField('Item_Fat_Content',StringType(),True),
                              StructField('Item_Visibility',DoubleType(),True),
                              StructField('Item_Type',StringType(),True),
                              StructField('Item_MRP',DoubleType(),True),
                              StructField('Outlet_Identifier',StringType(),True),
                              StructField('Outlet_Establishment_Year',IntegerType(),True),
                              StructField('Outlet_Size',StringType(),True),
                              StructField('Outlet_Location_Type',StringType(),True),
                              StructField('Outlet_Type',StringType(),True),
                              StructField('Item_Outlet_Sales',DoubleType(),True)
])

# COMMAND ----------

df=spark.read.format('csv')\
             .schema(my_strct_schema)\
             .option('header',True)\
             .load('/FileStore/tables/BigMart_Sales.csv')

# COMMAND ----------

df.printSchema()

# COMMAND ----------

df.display()

# COMMAND ----------

df_json.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Select
# MAGIC **This is use to select the required fields from dataframe**

# COMMAND ----------

df.display()

# COMMAND ----------

df.select('Item_Identifier','Item_Weight','Item_Fat_Content').display()

# COMMAND ----------

df.select(col('Item_Identifier'),col('Item_Weight'),col('Item_Fat_Content')).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### ALIAS

# COMMAND ----------

df.select(col('Item_Identifier').alias('Item_Id')).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### FILTER/WHERE

# COMMAND ----------

# MAGIC %md
# MAGIC #### Scenario-1

# COMMAND ----------

df.filter(col('Item_Fat_Content')=='Regular').display()

# COMMAND ----------

# MAGIC %md
# MAGIC #### Scenario-2

# COMMAND ----------

df.filter((col('Item_Type')== 'Soft Drinks') & (col('Item_Weight')<10)).display()

# COMMAND ----------

# MAGIC %md
# MAGIC #### Scenario-3

# COMMAND ----------

df.filter((col('Outlet_Size').isNull()) & (col('Outlet_Location_Type')\
                                     .isin('Tier 1','Tier 2'))).display()

# COMMAND ----------

df.filter((col('Outlet_Size').isNull()) | (col('Outlet_Location_Type').isin('Tier 1','Tier 2'))).display()

# | id or ; & is and ; ~ is not

# COMMAND ----------

# MAGIC %md
# MAGIC ### withColumnRenamed
# MAGIC
# MAGIC * This is different from .alias as alias will give new name but .withColumnRenamed will change the column name at dataframe level

# COMMAND ----------

df.withColumnRenamed('Item_Weight','Item_Wt').display()

# COMMAND ----------

# MAGIC %md
# MAGIC withColumn
# MAGIC
# MAGIC * This transformation helps to create new column or modify existing column

# COMMAND ----------

# MAGIC %md
# MAGIC #### Scenario 1: Creating New Column
# MAGIC **Part 1** : Creating new column with constant values

# COMMAND ----------

df.withColumn('flag',lit('new')).display()

# if we want new column to take constant value we need to use lit fuction

# COMMAND ----------

# MAGIC %md
# MAGIC **Part 2**: Creating new new column by dividing two cols

# COMMAND ----------

df.withColumn('divide',col('Item_Outlet_Sales')/col('Item_MRP')).display()

# COMMAND ----------

# MAGIC %md
# MAGIC #### Scenario 2: Modifying Existing Column
# MAGIC
# MAGIC  Item_Fat_Content={Regular: reg; Low Fat: lf}

# COMMAND ----------

df.withColumn('Item_Fat_Content',regexp_replace('Item_Fat_Content','Regular','Reg'))\
    .withColumn('Item_Fat_Content',regexp_replace(col('Item_Fat_Content'),'Low Fat','LF'))\
        .withColumn('Item_Type',regexp_replace(col('Item_Type'),'.*Goods.*' , 'Goods')).display()  #items that have goods in it will be replaced as good

# COMMAND ----------

# MAGIC %md
# MAGIC #### Type Casting

# COMMAND ----------

df.withColumn('Item_Weight',col('Item_Weight').cast(StringType())).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Sort or OrderBy

# COMMAND ----------

#Sorting descending of One column 
df.sort(col('Item_Weight').desc()).display()

# COMMAND ----------

#Sorting ascending of One column (ascending is default)
df.sort(col('Item_Visibility').asc()).display()

# COMMAND ----------

# Sorting desecding of two columns
df.sort(['Item_Weight','Item_Visibility'],ascending=[0,0]).display()

# COMMAND ----------

#Sorting desending of one column and ascending of one column
df.sort(['Item_Weight','Item_Visibility'],ascending=[0,1]).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Limit (Similar to head in pandas and limit in sql)

# COMMAND ----------

df.limit(10).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Drop

# COMMAND ----------

# Dropping One Column
df.drop('Item_Type').display()

# COMMAND ----------

#Dropping two columns
df.drop('Item_Type','Item_Visibility').display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Drop_duplicates

# COMMAND ----------

# dropping duplicates for all cols
df.drop_duplicates().display()

# COMMAND ----------

# dropping duplicates for selected cols
df.drop_duplicates(subset=['Item_Type','Item_MRP']).display()

# COMMAND ----------

df.drop_duplicates(['Item_Type']).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Distinct

# COMMAND ----------

df.distinct().display()

# COMMAND ----------

df.select(col('Item_Fat_Content')).distinct().display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Union And UnionByName

# COMMAND ----------

# MAGIC %md
# MAGIC #### Creating DataFrame For Union

# COMMAND ----------

data1=[('1','Karan'),('2','Arun')]
schema1='''
          id string,
          Name String
        '''
df1=spark.createDataFrame(data1,schema1)

# COMMAND ----------

data2=[('3','Maddy'),('4','Jerry')]

schema2=StructType([
                StructField('id',StringType(),False),
                StructField('Name',StringType(),True)  
])

df2=spark.createDataFrame(data2,schema2)

# COMMAND ----------

df1.display()

# COMMAND ----------

df2.display()

# COMMAND ----------

df1.union(df2).display()

# COMMAND ----------

df1.union(df2).printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ### unionByName
# MAGIC
# MAGIC * unionByName helps to union the two tables when columns are not in same order

# COMMAND ----------

data1=[('Karan','1'),('Arun','2')]
schema1='''
          Name string,
          id String
        '''
df1=spark.createDataFrame(data1,schema1)
df1.display()

# COMMAND ----------

df1.union(df2).display()

# COMMAND ----------

# As you
df1.unionByName(df2).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### String Functions
# MAGIC * INITCAP()
# MAGIC * UPPER()
# MAGIC * LOWER()

# COMMAND ----------

# initcap()
df.select(initcap('Item_Type')).display()

# COMMAND ----------

df.select(initcap(col('Item_Type'))).display()

# COMMAND ----------

# df.select(col(initcap('Item_Type'))).display()  -- TypeError: Column is not iterable

# COMMAND ----------

df.select(initcap(col('Item_Type')).alias('initcap_item_type')).display()

# COMMAND ----------

# upper()
df.withColumn('Item_Type',upper('Item_Type')).display()

# COMMAND ----------

# lower
df.withColumn('Item_Type',lower(col('Item_Type'))).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Date Functions
# MAGIC
# MAGIC * CURRENT_DATE()
# MAGIC * DATE_ADD()
# MAGIC * DATE_SUB()
# MAGIC * ADD_MONTHS()
# MAGIC * LAST_DAY()
# MAGIC * DATEDIFF()
# MAGIC * DATE_FORMAT()
# MAGIC * months_between()

# COMMAND ----------

# current_date()
df.withColumn('Curr_Date',current_date()).display()

# COMMAND ----------

df= df.withColumn('Curr_Date',current_date())
df.display()

# COMMAND ----------

#date_add
df.withColumn('week_after',date_add('curr_date',7)).display()

# COMMAND ----------

df.withColumn('week_before',date_add('curr_date',-7)).display()

# COMMAND ----------

#date_sub
df.withColumn('week_before',date_sub('curr_date',7)).display()

# COMMAND ----------

#add_months()
df.withColumn('month_after',add_months('curr_date',1)).display()

# COMMAND ----------

#last_day of previous month
#last_day() function
df.withColumn('last_day_prev_month',last_day(add_months('curr_date',-1))).display()

# COMMAND ----------

#first day of current month
#last_day() & dateadd() function
df.withColumn('first_day_curr_month',date_add(last_day(add_months('curr_date',-1)),1)).display()

# COMMAND ----------

#datediff
df.withColumn('datediff',datediff(date_add(last_day(add_months('curr_date',-1)),1),'Curr_Date')).display()

# COMMAND ----------

df.withColumn('datediff',datediff('Curr_Date',date_add(last_day(add_months('curr_date',-1)),1))).limit(10).display()

# COMMAND ----------

#date_format()
df.withColumn('dateformat',date_format('Curr_Date','dd-MM-yyyy')).limit(10).display()
#use only below formats
#dd-MM-yyyy (not YYYY)
#dd_MMM-yyyy

# COMMAND ----------

df.withColumn('dateformat',date_format('Curr_Date','dd-MMM-yyyy')).limit(10).display()

# COMMAND ----------

# extracting month using 'MMM-yy' format
df.withColumn('Month_Period',date_format('Curr_Date','MMM-yy')).limit(10).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Handling Nulls
# MAGIC
# MAGIC * dropping null values(dropna())
# MAGIC * filling null values (fillna())
# MAGIC

# COMMAND ----------

# dropping all null values

df.dropna('any').display()

# COMMAND ----------

# dropping all the records if all the columns are null values
df.dropna('all').display()

# COMMAND ----------

# dropping null values for particular column
df.dropna(subset=['Outlet_Size']).display()

# COMMAND ----------

# MAGIC %md
# MAGIC * Filling null values

# COMMAND ----------

df.fillna('NotAvaialable').display()

# COMMAND ----------

# filling null values for particular column
df.fillna(0,subset=['Item_Weight']).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Lowering column names

# COMMAND ----------

#lowering column names
for i in df.columns:
    print(i.lower())

# COMMAND ----------

#lowering all the field names and creating dataframe
df_lower=df.select([i.lower() for i in df.columns])
df_lower.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### split and Indexing

# COMMAND ----------

#split
df_lower.withColumn('outlet_type',split('outlet_type',' ')).display()

# COMMAND ----------

#indexing
df_lower.withColumn('outlet_type',split('outlet_type',' ')[0]).display()

# COMMAND ----------

df_json.display()

# COMMAND ----------

df_json.withColumn('forename',col('name')["forename"]).display()

# COMMAND ----------

df_json.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Explode
# MAGIC
# MAGIC * its explode a list into rows

# COMMAND ----------

df_exp=df_lower.withColumn('outlet_type',split('outlet_type',' '))
df_exp.display()

# COMMAND ----------

df_exp.printSchema()

# COMMAND ----------

#performing explode function
df_exp.withColumn('outlet_type',explode('outlet_type')).limit(10).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### array_contains()
# MAGIC * This function help to identify particular element is present in a list or array

# COMMAND ----------

df_exp.withColumn('type1_flag',array_contains('outlet_type','Type1')).limit(10).display()

# COMMAND ----------

# df_exp.withColumn('type1_flag',regexp_replace(col('outlet_type'),array_contains('outlet_type','Type1'),'Type1'))\
    #    .limit(10).display()

    #    TypeError: Column is not iterable

# COMMAND ----------

# MAGIC %md
# MAGIC ## groupBy

# COMMAND ----------

df_lower.limit(10).display()

# COMMAND ----------

# scenario -1 : groupBy

df_lower.groupBy('item_type').agg(sum('item_mrp')).display()

# COMMAND ----------

# scenario -2 : groupBy two columns and finding both sum and average of mrp
df_lower.groupBy('item_type','outlet_size').agg(sum('item_mrp').alias('total_mrp'),avg('item_mrp').alias('average_item_mrp')).display()

# COMMAND ----------

# scenario -3 : groupBy three columns and finding both sum of mrp and item outet sales
df_lower.groupBy('item_type','outlet_size','outlet_location_type')\
         .agg(sum('item_mrp').alias('total_mrp'),sum('item_outlet_sales').alias('total_outlet_sales')).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### collect_list
# MAGIC * this will is closely related to groupBy function but instead of aggregating it will create a list of items against the field
# MAGIC * like if we need a list of books read by particular user then we can use this

# COMMAND ----------

user_data=[('user 1','book 1'),('user 1','book 2'),
          ('user 2','book 2'),('user 3','book 4'),
          ('user 2','book 3'),('user 3','book 3')]

user_schema=StructType([
                         StructField('user',StringType(),False),
                         StructField('book',StringType(),True)
])

df_books=spark.createDataFrame(user_data,user_schema)
df_books.display()

# COMMAND ----------

# using a collect_list function
df_books.groupBy('user').agg(collect_list('book').alias('collection_of_books')).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Pivot table

# COMMAND ----------

# in pivot we will use *item_type* as row/index *outlet_size* as column and *item_mrp* as values
# groupBy is used to create index/row,pivot is used to create columns, agg() sis used to calculate values
df_lower.groupBy('item_type').pivot('outlet_size').agg(sum('item_mrp')).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### When-Otherwise
# MAGIC * This is similar to case-when in sql which is used to write conditional statement

# COMMAND ----------

df_lower.limit(10).display()

# COMMAND ----------

# scenario 1:
df_lower.withColumn('veg_flag',when(col('item_type')=='Meat','Non-Veg').otherwise('Veg')).display()

# COMMAND ----------

# outlet type identifier
df.withColumn('outlet_type_identifier',when(array_contains(split(col('outlet_type'),' '),'Type1'),'Type1')\
    .when(array_contains(split(col('outlet_type'),' '),'Type2'),'Type2').otherwise('not')).limit(10).display()

# COMMAND ----------

# Scenario 2
df_veg_flag=df_lower.withColumn('veg_flag',when(col('item_type')=='Meat','Non-Veg').otherwise('Veg'))
df_veg_flag.display()

# COMMAND ----------

df_veg_flag.withColumn('veg_expensive',when((col('veg_flag') == 'Veg') & (col('item_mrp')>=100),'expensive').\
             when((col('veg_flag') == 'Veg') & (col('item_mrp')<100),'inexpensive').otherwise('Non_veg')).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Joins
# MAGIC * Inner Join
# MAGIC * Right Join
# MAGIC * Left Join
# MAGIC * Full Join
# MAGIC * Anti Join

# COMMAND ----------

dataj1 = [('1','gaur','d01'),
          ('2','kit','d02'),
          ('3','sam','d03'),
          ('4','tim','d03'),
          ('5','aman','d05'),
          ('6','nad','d06')] 

schemaj1 = 'emp_id STRING, emp_name STRING, dept_id STRING' 

df1 = spark.createDataFrame(dataj1,schemaj1)

dataj2 = [('d01','HR'),
          ('d02','Marketing'),
          ('d03','Accounts'),
          ('d04','IT'),
          ('d05','Finance')]

schemaj2 = 'dept_id STRING, department STRING'

df2 = spark.createDataFrame(dataj2,schemaj2)

# COMMAND ----------

df1.display()

# COMMAND ----------

df2.display()

# COMMAND ----------

# inner join will map the common records from both the tables.
df1.join(df2,df1['dept_id']==df2['dept_id'],how='inner').display()

# COMMAND ----------

#left Join-- this join will take all the data from left and map the common records from reference table
df1.join(df2,df1['dept_id']==df2['dept_id'],how='left').select('emp_id',df1['dept_id'],'department').display()

# COMMAND ----------

#Right Join-- it is simply the opposite of left join
df1.join(df2,df1['dept_id']==df2['dept_id'],how='right').select('emp_id',df2['dept_id'],'department').display()

# COMMAND ----------

# Anti Join -- Whenever we want to fetch data from one DF which is not available in another DF then we use Anti Join

df1.join(df2,df1['dept_id']==df2['dept_id'],how='anti').display()

# COMMAND ----------

# full join
df1.join(df2,df1['dept_id']==df2['dept_id'],how='full').display()

# COMMAND ----------

empData = [(1,"Smith","2018",10,"M",3000),
    (2,"Rose","2010",20,"M",4000),
    (3,"Williams","2010",10,"M",1000),
    (4,"Jones","2005",10,"F",2000),
    (5,"Brown","2010",30,"",-1),
    (6,"Brown","2010",50,"",-1)
  ]
  
empColumns = ["emp_id","name","branch_id","dept_id",
  "gender","salary"]
empDF = spark.createDataFrame(empData,empColumns)
# empDF.show()

#DEPT DataFrame
deptData = [("Finance",10,"2018"),
    ("Marketing",20,"2010"),
    ("Marketing",20,"2018"),
    ("Sales",30,"2005"),
    ("Sales",30,"2010"),
    ("IT",50,"2010")
  ]
deptColumns = ["dept_name","dept_id","branch_id"]
deptDF=spark.createDataFrame(deptData,deptColumns)  
# deptDF.show()

# COMMAND ----------

empDF.display()

# COMMAND ----------

deptDF.display()

# COMMAND ----------

# Joining with multiple conditions

empDF.join(deptDF,(empDF['dept_id'] == deptDF['dept_id']) & (empDF['branch_id'] == deptDF['branch_id']),how='right')\
    .select('emp_id','name','dept_name',empDF['branch_id']).display()

# COMMAND ----------

# Joining using where
empDF.join(deptDF).where((empDF['dept_id'] == deptDF['dept_id']) & (empDF['branch_id'] == deptDF['branch_id'])).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Window Functions

# COMMAND ----------

# MAGIC %md
# MAGIC * **ROW_NUMBER()**
# MAGIC * Uses To remove duplicates and generate surrogate key

# COMMAND ----------

#importing window functions library
from pyspark.sql.window import Window

# COMMAND ----------

df_lower.withColumn('rownum',row_number().over(Window.orderBy(col('item_identifier')))).display()

# COMMAND ----------

#descending rownumber
df_lower.withColumn('rownum',row_number().over(Window.orderBy(desc(col('item_identifier'))))).display()

# COMMAND ----------

# MAGIC %md
# MAGIC * **Rank() & dense_rank()**

# COMMAND ----------

# rank and dense_rank descending order
df_lower.withColumn('rank',rank().over(Window.orderBy(col('item_identifier').desc())))\
         .withColumn('denserank',dense_rank().over(Window.orderBy(col('item_identifier').desc())))\
         .select(col('item_identifier'),col('rank'),col('denserank')).display()

# COMMAND ----------

# MAGIC %md
# MAGIC #### cumsum

# COMMAND ----------

df_lower.withColumn('cumsum',sum(col('item_mrp')).over(Window.orderBy(col('item_type'))))\
         .select(col('item_type'),col('item_mrp'),col('cumsum')).display()

# COMMAND ----------

df_lower.groupBy('item_type').agg(sum('item_mrp').alias('total_mrp')).display()

# COMMAND ----------

df_lower.groupBy('item_type').agg(sum('item_mrp').alias('total_mrp'))\
    .withColumn('new_col',sum(col('total_mrp')).over(Window.orderBy(col('item_type')))).display()

# COMMAND ----------

df_lower.withColumn('new_cumsum',sum('item_mrp').over(Window.orderBy('item_type')\
       .rowsBetween(Window.unboundedPreceding,Window.currentRow))).select('item_type','item_mrp','new_cumsum').display()

# COMMAND ----------

df_lower.withColumn('total_cumsum',sum('item_mrp').over(Window.orderBy('item_type')\
       .rowsBetween(Window.unboundedPreceding,Window.unboundedFollowing))).select('item_type','item_mrp','total_cumsum').display()

# COMMAND ----------

df_lower.withColumn('cumsum',sum('item_mrp').over(Window.partitionBy(col('item_type')).orderBy('item_identifier')))\
     .select('cumsum','item_type').display()

# COMMAND ----------

# MAGIC %md
# MAGIC #### User Defined Functions(UDF)

# COMMAND ----------

# MAGIC %md
# MAGIC * Step 1: Creating function in python

# COMMAND ----------

def my_func(x):
    return x*x

# COMMAND ----------

# MAGIC %md
# MAGIC * Step2:Converting into pyspark udf

# COMMAND ----------

my_udf=udf(my_func)

# COMMAND ----------

df_lower.withColumn('square_col',my_udf(col('item_mrp'))).select('item_mrp','square_col').display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Data Writing

# COMMAND ----------

#df.write.format('csv').save('/FileStore/tables/data.csv')

# COMMAND ----------

dbutils.fs.ls('/FileStore/tables/')

# COMMAND ----------

#df_lower.write.format('csv').save('/FileStore/tables/data.csv')

# COMMAND ----------

# MAGIC %md
# MAGIC ### Writing Modes
# MAGIC * append
# MAGIC * overwrite
# MAGIC * error
# MAGIC * ignore

# COMMAND ----------

# append - this mode will append the similar file name into the storage whether or not the file exists
df_lower.write.format('csv').mode('append').save('/FileStore/tables/data.csv')

# COMMAND ----------

dbutils.fs.ls('/FileStore/tables/')  # two similar filename

# COMMAND ----------

## Overwrite- this will overwrite the existng file and create new one
df_lower.write.format('csv').mode('overwrite')\
      .option('path','/FileStore/tables/data.csv').save()

# COMMAND ----------

dbutils.fs.ls('/FileStore/tables/') 

# COMMAND ----------

# error - this will throws error if similar file name exists
df_lower.write.format('csv').mode('error')\
      .option('path','/FileStore/tables/data.csv').save()

# COMMAND ----------

# ignore - it will ignore if the similar file name exists neither it will append the file nor overwrite the file or it donnot show error also

df_lower.write.format('csv').mode('ignore')\
      .option('path','/FileStore/tables/data.csv').save()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Parquet File Format

# COMMAND ----------

# writing data in paquet file format
df_lower.write.format('parquet').mode('overwrite')\
      .option('path','/FileStore/tables/data.parquet').save()

# COMMAND ----------

#delta file format
# df_lower.write.format('delta').mode('overwrite')\
#       .option('path','/FileStore/tables/data.parquet').save()

# COMMAND ----------

# MAGIC %md
# MAGIC ### tables

# COMMAND ----------

df_lower.write.format('delta').mode('overwrite')\
      .saveAsTable('my_table')

# COMMAND ----------

# MAGIC %md
# MAGIC #### create or replace temp view

# COMMAND ----------

df_lower.createTempView('my_view')

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from my_view where item_fat_content='LF'

# COMMAND ----------

df_sql= spark.sql("select * from my_view where item_fat_content='LF'")

# COMMAND ----------

df_sql.limit(10).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Upsert query in pyspark
# MAGIC     `(deltaTable.alias("people_10m")
# MAGIC     .merge(
# MAGIC     people_10m_updates.alias("people_10m_updates"),
# MAGIC     "people_10m.id = people_10m_updates.id")
# MAGIC     .whenMatchedUpdateAll()
# MAGIC     .whenNotMatchedInsertAll()
# MAGIC     .execute())`
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ### Upsert query in sql
# MAGIC     `MERGE INTO people_10m
# MAGIC     USING people_10m_updates
# MAGIC     ON people_10m.id = people_10m_updates.id
# MAGIC     WHEN MATCHED THEN UPDATE SET *
# MAGIC     WHEN NOT MATCHED THEN INSERT *;`

# COMMAND ----------

# MAGIC %md
# MAGIC ### update query in pyspark
# MAGIC * Declare the predicate by using a SQL-formatted string
# MAGIC     `deltaTable.update(
# MAGIC     condition = "gender = 'F'",
# MAGIC     set = { "gender": "'Female'" }
# MAGIC     )`
# MAGIC
# MAGIC * Declare the predicate by using Spark SQL functions.
# MAGIC     `deltaTable.update(
# MAGIC     condition = col('gender') == 'M',
# MAGIC     set = { 'gender': lit('Male') }
# MAGIC     )`

# COMMAND ----------

# deltaTable.delete("birthDate < '1955-01-01'")   --- delete query in pyspark

# COMMAND ----------

# MAGIC %md
# MAGIC ## optimize table
# MAGIC * After you have performed multiple changes to a table, you might have a lot of small files. To improve the speed of read queries, you can use the optimize operation to collapse small files into larger ones:
# MAGIC     
# MAGIC     `deltaTable.optimize().executeCompaction()`
# MAGIC   * using sql  
# MAGIC     `OPTIMIZE main.default.people_10m`
# MAGIC
# MAGIC * To improve read performance further, you can collocate related information in the same set of files by z-ordering. Delta Lake data-skipping algorithms use this collocation to dramatically reduce the amount of data that needs to be read. To z-order data, you specify the columns to order on in the z-order by operation. For example, to collocate by gender, run:
# MAGIC
# MAGIC     `deltaTable.optimize().executeZOrderBy("gender")`
# MAGIC * using sql
# MAGIC     `OPTIMIZE main.default.people_10m ZORDER BY (gender)`

# COMMAND ----------

# MAGIC %md
# MAGIC ### writing data using partitionBy   
# MAGIC     `df.write.format("delta").mode("overwrite").partitionBy("Month").save(path)`
# MAGIC
# MAGIC
# MAGIC     `df2.write.format("delta").mode("append").partitionBy("Month").save(path)`

# COMMAND ----------

# MAGIC %md
# MAGIC ### Create DataFrame representing the stream of input lines from connection to localhost:9999
# MAGIC     `spark \
# MAGIC     .readStream \
# MAGIC     .format("socket") \
# MAGIC     .option("host", "localhost") \
# MAGIC     .option("port", 9999) \
# MAGIC     .load()`

# COMMAND ----------

# MAGIC %md
# MAGIC #### Start running the query that prints the running counts to the console
# MAGIC     `wordCounts \
# MAGIC     .writeStream \
# MAGIC     .outputMode("complete") \
# MAGIC     .format("console") \
# MAGIC     .start()`

# COMMAND ----------

