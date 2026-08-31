
"""AWS Glue ETL Job: Create a table categorising sum spending into tiers
Purpose:
    Reads the targeted S3 and then calculated the sum spending based on provided transaction.
    This is ideally a transaction database. When certain thresholds are met, the tier upgrades
    where they remain indefinitely. The customer can upgrade tier as follows:
    Bronze >> Silver >> Gold
    Each tier has a Valid From and Valid To date.  The highest tier is Gold and they cannot be
    demoted or reach the Valid To date as the current tier they sit in goes to 31-12-9999

How to use:
    1. Point to source S3. In my case, full_data_parquet
    2. Change column assigned with the sum value of each transaction. Might need to be calculated.
        TotalAmount is what it is called in my database
    3. Update customerID to your unique ID
    4. Point to targeted output table. In my case, loyalty-tier 

"""

#----------Setting up work space----------------
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



#---Grabbing S3/ Data source location
#transactions_df = spark.read.parquet("s3://roshan-de-practice-2026/clean_parquet/") used for testing on a smaller sample
transactions_df = spark.read.parquet("s3://roshan-de-practice-2026/full_data_parquet/")
#----------assigning to df2 to start transformations 
df2=transactions_df


#--------Creating columns I potentially need. Has Transaction Date and Time. Not used but can be integrated if needed. 
df3 =(df2.withColumn("TransactionDate",F.to_timestamp(F.col("TransactionDate"), "M/d/yyyy H:mm"))
        .withColumn("TotalAmount", F.col("TotalAmount").cast("double"))
  #      .withColumn("TransDate",F.to_date(F.col("TransactionDate")))
    #    .withColumn("TransTime",F.date_format(F.col("TransactionDate"),"HH:mm"))
      )


#---------Creating spend aggregation column per individual
#wind_func=(Window.partitionBy(F.col("CustomerID")).orderBy(F.col("TransactionDate").asc())).rowsBetween(Window.unboundedPreceding, Window.currentRow))
wind_func = (Window.partitionBy(F.col("CustomerID")).orderBy(F.col("TransactionDate").asc()).rowsBetween(Window.unboundedPreceding, Window.currentRow))
df4=(df3.withColumn("Spend_sum",F.sum(F.col("TotalAmount")).over(wind_func))
.filter(F.col("CustomerID").isNotNull()))


#------Assigning tiers to Spend Sum value
df5=(df4.withColumn("tier",F.when(F.col("Spend_sum") <500, "Bronze")
                           .when(F.col("Spend_sum") <1500, "Silver")
                           .otherwise( "Gold"))
         .withColumn("tier_val",F.when(F.col("Spend_sum") <500, 1)
                           .when(F.col("Spend_sum") <1500, 2)
                           .otherwise( 3)))



#---------Using .lag() to detect change from previous tier
lag_wind=Window.partitionBy(F.col("CustomerID")).orderBy(F.col("TransactionDate").asc())
df6=(df5.withColumn("prev_tier_val",F.lag(F.col("tier_val")).over (lag_wind)))





#Code to check change in lag and then flag it as ChangeFlag. Then add these up as runningID. the non change transaction will be valued at 0 so wont change.
#Each spending tier will have its own runningID assigned starting from 1
wind_func=Window.partitionBy(F.col("CustomerID")).orderBy("TransactionDate")
df7=(df6.withColumn("ChangeFlag",F.when(F.col("prev_tier_val").isNull() ,1)
                                 .when(F.col("tier_val")!=F.col("prev_tier_val"),1)
                                .otherwise(0))
        .withColumn("RunningID", F.sum("ChangeFlag").over(wind_func)))



#groupby runningID then getting min and max dates. Can remove Max group as it isnt needed as there is a gap between the highest date in a tier and the transaction that upgrades them. 
max_func=Window.partitionBy(F.col("CustomerID")).orderBy(F.col("valid_from"))
df8=(df7.groupBy(F.col("CustomerID"),F.col("RunningID"),F.col("tier"),F.col("tier_val"))
        .agg((F.min(F.col("TransactionDate"))).alias("valid_from"),(F.max(F.col("TransactionDate"))).alias("valid_to_raw")))
        
#Getting the next min date in the upcoming tier as a column. When null, that means it is the most recent tier. Then check if the next tier is null, assign the max expiry (9999-12-31 because current tier)
# otherwise that was an old tier. The maximum time in that tier is the lead value -1 day
df9=(df8.withColumn("next_valid_from",F.lead(F.col("valid_from")).over(max_func)))
df10=(df9.withColumn("valid_to",F.when(F.col("next_valid_from").isNull(),F.lit("9999-12-31").cast("date"))
                                 .otherwise(F.date_sub(F.col("next_valid_from"),1)))
          .withColumn("is_current",F.when(F.col("next_valid_from").isNull(),"Y")
                                 .otherwise("N")))
#- df10 select the columns I want. Drop scaffolding columns. 
df10 = df10.drop("valid_to_raw", "next_valid_from")



df10.write.mode("overwrite").parquet("s3://roshan-de-practice-2026/loyalty-tier/")

job.commit()






