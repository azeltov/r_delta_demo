#install.packages("sparklyr")
library(sparklyr)
#spark_install(version = 3.2)

conf <- spark_config()

conf$spark.driver.memory = "4G"
conf$spark.driver.memoryOverhead = "4g" 
conf$spark.executor.memory = "4G"
conf$spark.executor.memoryOverhead = "1g"

conf$sparklyr.defaultPackages <- c("io.delta:delta-core_2.12:1.0.0",
                                   "org.apache.spark:spark-sql_2.13:3.2.0")
conf
options(sparklyr.log.console = TRUE)
sc <- spark_connect(master = "local", version = "3.2",  config = conf)

#sc <- spark_connect(master = "local", version = "3.2",  packages = "io.delta:delta-core_2.12:1.0.0,org.apache.spark:spark-sql_2.13:3.2.0 ")
path = "file:///home/azureuser/cloudfiles/data/datastore/rdelta/safedriverdata_delta/part-00000-6457272b-ca2d-495b-8339-bd52b93ed79e-c000.snappy.parquet"
spark_df =spark_read_parquet(sc,name="df",path)
spark_df

delta_path = "file:///home/azureuser/cloudfiles/data/datastore/rdelta/safedriverdata_delta/"
delta_spark_df =spark_read_delta(sc,name="delta",delta_path)
delta_spark_df
q <- 'select * from delta limit 10'

sql= sdf_sql(sc,q)
sql

r_df <- sql %>%  collect()
r_df

spark_disconnect(sc) 
