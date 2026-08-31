#%help

%idle_timeout 15

import sys
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

from pyspark.sql import functions as F
from pyspark.sql.window import Window


args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)
%idle_timeout 15



#transactions_df = spark.read.parquet("s3://roshan-de-practice-2026/clean_parquet/") used for testing on a smaller sample
transactions_df = spark.read.parquet("s3://roshan-de-practice-2026/full_data_parquet/")

df2=transactions_df



df2.show (100)


transactions_df.printSchema()


df2=transactions_df
df3 =(df2.withColumn("TransactionDate",F.to_timestamp(F.col("TransactionDate"), "M/d/yyyy H:mm"))
        .withColumn("TotalAmount", F.col("TotalAmount").cast("double"))
        .withColumn("TransDate",F.to_date(F.col("TransactionDate")))
        .withColumn("TransTime",F.date_format(F.col("TransactionDate"),"HH:mm")))

df3.show()


df3.printSchema()



#wind_func=(Window.partitionBy(F.col("CustomerID")).orderBy(F.col("TransactionDate").asc())).rowsBetween(Window.unboundedPreceding, Window.currentRow))
wind_func = (Window.partitionBy(F.col("CustomerID")).orderBy(F.col("TransactionDate").asc()).rowsBetween(Window.unboundedPreceding, Window.currentRow))
df4=(df3.withColumn("Spend_sum",F.sum(F.col("TotalAmount")).over(wind_func))
.filter(F.col("CustomerID").isNotNull()))
df4.show()

df4.printSchema()


df5=(df4.withColumn("tier",F.when(F.col("Spend_sum") <500, "Bronze")
                           .when(F.col("Spend_sum") <1500, "Silver")
                           .otherwise( "Gold"))
         .withColumn("tier_val",F.when(F.col("Spend_sum") <500, 1)
                           .when(F.col("Spend_sum") <1500, 2)
                           .otherwise( 3)))
df5.show()



lag_wind=Window.partitionBy(F.col("CustomerID")).orderBy(F.col("TransactionDate").asc())
df6=(df5.withColumn("prev_tier_val",F.lag(F.col("tier_val")).over (lag_wind)))
    # withColumn("Running_ID",
df6.select("CustomerID","TransDate","TransTime","Spend_sum","tier","tier_val","prev_tier_val").orderBy("CustomerID").show()



#Code to check change in lag
wind_func=Window.partitionBy(F.col("CustomerID")).orderBy("TransactionDate")
df7=(df6.withColumn("Changeflag",F.when(F.col("prev_tier_val").isNull() ,1)
                                 .when(F.col("tier_val")!=F.col("prev_tier_val"),1)
                                .otherwise(0))
        .withColumn("RunningID", F.sum("ChangeFlag").over(wind_func))
    
    
    )
df7.select("CustomerID","TransactionDate","TransDate","TransTime","Spend_sum","tier","tier_val","prev_tier_val","Changeflag","RunningID").show()


#groupby with max
max_func=Window.partitionBy(F.col("CustomerID")).orderBy(F.col("valid_from"))
df8=(df7.groupBy(F.col("CustomerID"),F.col("RunningID"),F.col("tier"),F.col("tier_val"))
        .agg((F.min(F.col("TransactionDate"))).alias("valid_from"),(F.max(F.col("TransactionDate"))).alias("valid_to_raw")))
        
     
df9=(df8.withColumn("next_valid_from",F.lead(F.col("valid_from")).over(max_func)))
df10=(df9.withColumn("valid_to",F.when(F.col("next_valid_from").isNull(),F.lit("9999-12-31").cast("date"))
                                 .otherwise(F.date_sub(F.col("next_valid_from"),1)))
          .withColumn("is_current",F.when(F.col("next_valid_from").isNull(),"Y")
                                 .otherwise("N")))
df10 = df10.drop("valid_to_raw", "next_valid_from")
df10.show()


max_func=Window.partitionBy(F.col("CustomerID")).orderBy(F.col("valid_from"))
df8=(df7.groupBy(F.col("CustomerID"),F.col("RunningID"),F.col("tier"),F.col("tier_val"))
        .agg((F.min(F.col("TransactionDate"))).alias("valid_from"),(F.max(F.col("TransactionDate"))).alias("valid_to_raw")))
        
     
df9=(df8.withColumn("next_valid_to",F.lead(F.col("valid_from")).over(max_func)))
df10=(df9.withColumn("valid_to",F.when(F.col("next_valid_to").isNull(),F.lit("9999-12-31").cast("date"))
                                 .otherwise(F.date_sub(F.col("next_valid_to"),1)))
            .withColumn("is_current",F.when(F.col("next_valid_to").isNull(),"Y")
                                 .otherwise("N")))
df10.orderBy("CustomerID").filter(F.col("tier_val") >2).show()


df10.filter(F.col("CustomerID")==340516).show()


df7.select(F.max("Spend_sum").alias("max_spend"), F.min("Spend_sum").alias("min_spend")).show()
df7.groupBy("CustomerID").count().orderBy(F.col("count").desc()).show(5)

#df7=(df6.select(F.col("CustomerID"),F.col("Spend_sum"),F.col("tier"),F.col("tier_val"))
      #  .withColumn("Max

df10.write.mode("overwrite").parquet("s3://roshan-de-practice-2026/loyalty-tier/")

job.commit()






