from pyspark.sql.types import *
from pyspark.sql import SparkSession
from pyspark.sql.functions import lit

spark  = SparkSession.builder.master("local").enableHiveSupport().getOrCreate()
spark.sparkContext.setLogLevel('WARN')
sc = spark.sparkContext

def align_sequence_lines(line):
    line_arr = line[0].split(",")
    if (len(line_arr) > 1):
        return line


#Mention sequence file path
data_rdd = sc.sequenceFile("<input_path>")

data_norm_rdd = (data_rdd.flatMap(lambda l: l).map(lambda l: l.split("\n")).map(lambda x: align_sequence_lines(x))).filter(lambda x: x is not None).flatMap(lambda l: l).map(lambda x: x.split(','))

#Mention sequence file data schema
_data_schema_text = "eventtime|eventduration|subscribermsisdn|downlinkusage|uplinkusage|datavolumetotal|subscriberimsi|rat|sender|receiver|sessionid|filename|charge|taxrate|discountrate|currency|tethering"

#Add schema to sequence file
data_schema=StructType([StructField(field_name, StringType(), True) for field_name in _data_schema_text.split("|")])

#Create dataframe
data_df_static = spark.createDataFrame(data_norm_rdd, data_schema).withColumn('tap_gprs', lit('tap_gprs')).withColumn('cdr_type',lit('gprs')).withColumn('duration',lit(0))
data_df =data_df_static.withColumn('dataVolumeIncoming',data_df_static['downlinkusage']).withColumn('dataVolumeOutgoing',data_df_static['uplinkusage']).select('tap_gprs','cdr_type','sender','receiver','duration','dataVolumeIncoming','dataVolumeOutgoing','filename')

#Register table over the dataframe
data_df.createOrReplaceTempView("outroamer_dm")

# Run sql query to get the desired result and save as orc file
spark.sql("select filename, sender, receiver, cdr_type, count(*) as num_rec, sum(duration) as duration, sum(dataVolumeIncoming) as downlinkusage, sum(dataVolumeOutgoing) as uplinkusage from outroamer_dm group by filename, sender, receiver, cdr_type order by 1,2,3,4").write.format("orc").save("<output_path>")
