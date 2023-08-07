from delta import *
from video_rppg_extraction import VideoRppgExtraction
from signal_processer import SignalProcessor
from pyspark.sql.types import *
from pyspark.sql.functions import col, max
import pyspark
from datetime import datetime, timedelta
import numpy as np
from minio import Minio
import time
import subprocess
from settings import RAW_BUCKET_NAME, S3A_ENDPOINT, LANDING_BUCKET_NAME
from minio.commonconfig import CopySource


builder = pyspark.sql.SparkSession.builder \
    .appName("MyApp") \
    .master("local[*]") \
    .config("spark.jars.packages", "org.apache.hadoop.fs.s3a.S3AFileSystem, org.apache.hadoop.hadoop-aws") \
    .config("spark.driver.memory", "10g") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .config("spark.sql.warehouse.dir", "delta") \
    .config("spark.hadoop.fs.s3a.endpoint", S3A_ENDPOINT) \
    .config("spark.hadoop.fs.s3a.access.key", "minioadmin") \
    .config("spark.hadoop.fs.s3a.secret.key", "minioadmin") \
    .config("spark.hadoop.fs.s3a.path.style.access", True) \
    .config("spark.hadoop.fs.s3a.fas.upload", True) \
    .config("spark.hadoop.fs.s3a.multipart.size", 104857608) \
    .config("fs.s3a.connection.maximum", 100) \
    .config("spark.delta.logStore.class", "org.apache.spark.sql.delta.storage.S3SingleDriverLogStore")  \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.databricks.delta.properties.defaults.enableChangeDataFeed", "true") \

spark = configure_spark_with_delta_pip(builder, ["org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.1", "org.apache.hadoop:hadoop-aws:3.3.1"]).getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

client = Minio("localhost:9000", access_key="minioadmin", secret_key="minioadmin", secure=False)



#####################################################################################
# Initialize necessary classes
#####################################################################################

video_extractor = VideoRppgExtraction()
signal_processer = SignalProcessor()



#####################################################################################
# Define schema for DataFrame
#####################################################################################
filtered_bvp_schema = StructType(
    [
        StructField("filtered_bvp", ArrayType(DoubleType())),
        StructField("fps", DoubleType()),
        StructField("start_timestamp", TimestampType()),
        StructField("end_timestamp", TimestampType()),
        StructField("subject_uuid", StringType()),
        StructField("video", StringType())
    ]
)
(
DeltaTable.createIfNotExists(spark)
    .location("s3a://trusted-zone/trusted_filtered_bvp_signal")
    .tableName("trusted_filtered_bvp_signal")
    .addColumns(filtered_bvp_schema)
    .execute()
) 


trusted_heartpy_measures_schema = StructType([
    StructField("bpm", DoubleType(), True),
    StructField("breathingrate", DoubleType(), True),
    StructField("end_timestamp", TimestampType(), True),
    StructField("hr_mad", DoubleType(), True),
    StructField("ibi", DoubleType(), True),
    StructField("pnn20", DoubleType(), True),
    StructField("pnn50", DoubleType(), True),
    StructField("rmssd", DoubleType(), True),
    StructField("s", DoubleType(), True),
    StructField("sd1", DoubleType(), True),
    StructField("sd1/sd2", DoubleType(), True),
    StructField("sd2", DoubleType(), True),
    StructField("sdnn", DoubleType(), True),
    StructField("sdsd", DoubleType(), True),
    StructField("start_timestamp", TimestampType(), True),
    StructField("subject_uuid", StringType(), True),
    StructField("video", StringType(), True)
])
(DeltaTable.createIfNotExists(spark)
    .location("s3a://trusted-zone/trusted_heartpy_measures")
    .tableName("trusted_heartpy_measures")
    .addColumns(trusted_heartpy_measures_schema)
    .execute()
)


trusted_working_data_schema = StructType([
    StructField("RR_diff", ArrayType(DoubleType(), True), True),
    StructField("RR_list", ArrayType(DoubleType(), True), True),
    StructField("RR_sqdiff", ArrayType(DoubleType(), True), True),
    StructField("breathing_frq", ArrayType(DoubleType(), True), True),
    StructField("breathing_psd", ArrayType(DoubleType(), True), True),
    StructField("breathing_signal", ArrayType(DoubleType(), True), True),
    StructField("end_timestamp", TimestampType(), True),
    StructField("hr", ArrayType(DoubleType(), True), True),
    StructField("nn20", ArrayType(DoubleType(), True), True),
    StructField("nn50", ArrayType(DoubleType(), True), True),
    StructField("removed_beats_y", ArrayType(DoubleType(), True), True),
    StructField("rolling_mean", ArrayType(DoubleType(), True), True),
    StructField("rrsd", DoubleType(), True),
    StructField("sample_rate", DoubleType(), True),
    StructField("start_timestamp", TimestampType(), True),
    StructField("subject_uuid", StringType(), True),
    StructField("video", StringType(), True),
    StructField("ybeat", ArrayType(DoubleType(), True), True),
])
(
DeltaTable.createIfNotExists(spark)
    .location("s3a://trusted-zone/trusted_working_data")
    .tableName("trusted_working_data")
    .addColumns(trusted_working_data_schema)
    .execute()
)


refined_breathing_data_schema = StructType([
    StructField('breathing_frq', ArrayType(DoubleType()), True),
    StructField('breathing_psd', ArrayType(DoubleType()), True),
    StructField('breathing_rate', LongType(), True),
    StructField('breathing_signal', ArrayType(DoubleType()), True),
    StructField('end_timestamp', TimestampType(), True),
    StructField('start_timestamp', TimestampType(), True),
    StructField('subject_uuid', StringType(), True),
    StructField('video', StringType(), True),
])
(
DeltaTable.createIfNotExists(spark)
    .location("s3a://refined-zone/refined_breathing_data")
    .tableName("refined_breathing_data")
    .addColumns(refined_breathing_data_schema)
    .execute()
)


refined_psd_frequencies_data_schema = StructType([
    StructField("freq_ULF", ArrayType(DoubleType()), True),
    StructField("power_ULF", ArrayType(DoubleType()), True),
    StructField("freq_VLF", ArrayType(DoubleType()), True),
    StructField("power_VLF", ArrayType(DoubleType()), True),
    StructField("freq_LF", ArrayType(DoubleType()), True),
    StructField("power_LF", ArrayType(DoubleType()), True),
    StructField("freq_HF", ArrayType(DoubleType()), True),
    StructField("power_HF", ArrayType(DoubleType()), True),
    StructField("freq_VHF", ArrayType(DoubleType()), True),
    StructField("power_VHF", ArrayType(DoubleType()), True),
    StructField("start_timestamp", TimestampType(), True),
    StructField("end_timestamp", TimestampType(), True),
    StructField("subject_uuid", StringType(), True),
    StructField("video", StringType(), True)
])
(
DeltaTable.createIfNotExists(spark)
    .location("s3a://refined-zone/refined_psd_frequencies_data")
    .tableName("refined_psd_frequencies_data")
    .addColumns(refined_psd_frequencies_data_schema)
    .execute()
) 


refined_rri_histogram_data_schema = StructType([
    StructField("bins", ArrayType(DoubleType(), containsNull=True)),
    StructField("counts", ArrayType(LongType(), containsNull=True)),
    StructField("end_timestamp", TimestampType(), nullable=True),
    StructField("rri", ArrayType(DoubleType(), containsNull=True)),
    StructField("start_timestamp", TimestampType(), nullable=True),
    StructField("subject_uuid", StringType(), nullable=True),
    StructField("video", StringType(), nullable=True)
])
(
DeltaTable.createIfNotExists(spark)
    .location("s3a://refined-zone/refined_rri_histogram_data")
    .tableName("refined_rri_histogram_data")
    .addColumns(refined_rri_histogram_data_schema)
    .execute()
)


refined_hr_data_schema = StructType(
    [
        StructField("hr", ArrayType(DoubleType())),
        StructField("fps", DoubleType()),
        StructField("bpm", LongType()),
        StructField("start_timestamp", TimestampType()),
        StructField("end_timestamp", TimestampType()),
        StructField("subject_uuid", StringType()),
        StructField("video", StringType()),
    ]
)
(
DeltaTable.createIfNotExists(spark)
    .location("s3a://refined-zone/refined_hr_data")
    .tableName("refined_hr_data")
    .addColumns(refined_hr_data_schema)
    .execute()
)

trusted_sessions_schema = StructType(
    [
        StructField("id", LongType()),
        StructField("start_timestamp", TimestampType()),
        StructField("end_timestamp", TimestampType()),
        StructField("subject_uuid", StringType()),
        StructField("video", StringType()),
    ]
)
(
DeltaTable.createIfNotExists(spark)
    .location("s3a://trusted-zone/trusted_sessions")
    .tableName("trusted_sessions")
    .addColumns(trusted_sessions_schema)
    .execute()
)






#####################################################################################
## Function that processed the video to extract BVP Signal using rPPG
#####################################################################################

# Extract BVP signal from a video
def process_video(new_filename):
    
    print("Processing: ", new_filename)

    subject_uuid = str(new_filename).split("_")[0]
    tmp_video_path = "/tmp/" + str(new_filename)

    client.fget_object(RAW_BUCKET_NAME, str(new_filename), tmp_video_path)

    # TODO: Decrypt video first

    filtered_bvp, fps, duration = video_extractor.extract_bvp_signal(tmp_video_path)

    # Map all values from np.float64 to python float
    filtered_bvp = list(map(float, filtered_bvp))

    # Create and store data frame into a delta table
    start_timestmap = datetime.now()
    data = {
        "filtered_bvp": filtered_bvp,
        "fps": fps,
        "start_timestamp": start_timestmap,
        "end_timestamp": start_timestmap+timedelta(seconds=duration),
        "subject_uuid": subject_uuid,
        "video": new_filename,
    }
    filtered_bvp_df = spark.createDataFrame(data=[data])
    filtered_bvp_df.write.format("delta").mode("append").save(
        "s3a://trusted-zone/trusted_filtered_bvp_signal"
    )

    # Get the maximum ID from the table
    session_df = spark.read.format("delta").load("s3a://trusted-zone/trusted_sessions")
    if session_df.rdd.isEmpty():
        max_id = -1
    else:
        max_id = session_df.select(max(col("id"))).collect()[0][0]

    # Add a new session row to the sessions table
    session_data = {
        "id": max_id + 1,
        "start_timestamp": start_timestmap,
        "end_timestamp": start_timestmap+timedelta(seconds=duration),
        "subject_uuid": subject_uuid,
        "video": new_filename,
    }
    session_data_df = spark.createDataFrame(data=[session_data])
    session_data_df.write.format("delta").mode("append").save(
        "s3a://trusted-zone/trusted_sessions"
    )

    subprocess.call(["rm", tmp_video_path])
    print("Processing over")






#####################################################################################
## Waiting for a new BVP Signal
### Then processes it and stores measures and working data into new delta tables
#####################################################################################

filtered_bvp_signal_stream_changes_df = (
    spark.readStream
		.format("delta") 
    	.option("readChangeFeed", "true") 
	    .option("startingVersion", "latest")
	    .option("path", "s3a://trusted-zone/trusted_filtered_bvp_signal")
	    .load()
)

def process_batch(df, epoch_id):
	# Collect all rows as a list of Rows
	rows = df.collect()

	# Iterate over the list of Rows and do the processing
	for row in rows:
		# Process extracted BVP signal (BPM, HRV ...)
		tmp_working_data, measures = signal_processer.process_bvp_signal(row["filtered_bvp"], row["fps"])

		# Change data types of working data, to be compatible with spark df
		working_data = dict()
		for key in list(tmp_working_data.keys()):
			if isinstance(tmp_working_data[key], np.ndarray):
				if isinstance(tmp_working_data[key][0], float):
					working_data[key] = [float(v) for v in tmp_working_data[key]]
				elif isinstance(tmp_working_data[key][0], int):
					working_data[key] = [int(v) for v in tmp_working_data[key]]
			elif isinstance(tmp_working_data[key], float):
				working_data[key] = float(tmp_working_data[key])


		# CREATE MEASURES DATA FRAME
		measures["start_timestamp"] = row["start_timestamp"]
		measures["end_timestamp"] = row["end_timestamp"]
		measures["subject_uuid"] = row["subject_uuid"]
		measures["video"] = row["video"]
		measures_df = spark.createDataFrame(data=[measures])
		# --------------------


		# CREATE WORKING_DATA DATA FRAME
		working_data["start_timestamp"] = row["start_timestamp"]
		working_data["end_timestamp"] = row["end_timestamp"]
		working_data["subject_uuid"] = row["subject_uuid"]
		working_data["video"] = row["video"]
		working_data_df = spark.createDataFrame(data=[working_data])
		# --------------------


		# CREATE BREATHING SIGNAL DATA FRAME
		breathing_data = dict()
		breathing_data["start_timestamp"] = row["start_timestamp"]
		breathing_data["end_timestamp"] = row["end_timestamp"]
		breathing_data["subject_uuid"] = row["subject_uuid"]
		breathing_data["video"] = row["video"]
		breathing_data["breathing_signal"] = working_data["breathing_signal"]
		breathing_data["breathing_psd"] = working_data["breathing_psd"]
		breathing_data["breathing_frq"] = working_data["breathing_frq"]
		breathing_data["breathing_rate"] = round(60 / (1 / measures["breathingrate"])) #measures["breathingrate"]				# manter trusted ou refined? TODO:
		# TODO: Have the index as a column too. To build the plot with no transfomations needed?		(datetime.datetime)			????
		breathing_data_df = spark.createDataFrame(data=[breathing_data])
		# --------------------


		# CREATE HR DATA FRAME
		hr_data = dict()
		hr_data["start_timestamp"] = row["start_timestamp"]
		hr_data["end_timestamp"] = row["end_timestamp"]
		hr_data["subject_uuid"] = row["subject_uuid"]
		hr_data["video"] = row["video"]
		hr_data["hr"] = working_data["hr"]
		hr_data["fps"] = row["fps"]
		hr_data["bpm"] = round(measures["bpm"]) #measures["bpm"]
		hr_data_df = spark.createDataFrame(data=[hr_data])
		# --------------------


		# CREATE RRI HISTOGRAM DATAFRAME
		n_bins = int(np.ceil(np.log2(len(working_data["RR_list"]))) + 1)
		counts, bins = np.histogram(working_data["RR_list"], n_bins)
		
		rri_histogram_data = dict()
		rri_histogram_data["start_timestamp"] = row["start_timestamp"]
		rri_histogram_data["end_timestamp"] = row["end_timestamp"]
		rri_histogram_data["subject_uuid"] = row["subject_uuid"]
		rri_histogram_data["video"] = row["video"]
		rri_histogram_data["rri"] = working_data["RR_list"]
		rri_histogram_data["counts"] = [int(c) for c in counts]
		rri_histogram_data["bins"] = [float(b) for b in bins]
		rri_histogram_data_df = spark.createDataFrame(data=[rri_histogram_data])
		# --------------------


		# CREATE PSD FREQUENCIES DATAFRAME
		freq, power, frequency_band_index, labels = signal_processer.get_psd_frequencies(tmp_working_data["peaklist"], tmp_working_data["RR_list"], row["fps"])

		psd_frequencies_data = dict()
		for band_index, label in zip(frequency_band_index, labels):

			freq_key = "freq_"+str(label)
			psd_frequencies_data[freq_key] = [float(v) for v in freq[band_index]]

			power_key = "power_"+str(label)
			psd_frequencies_data[power_key] = [float(v) for v in power[band_index]]

		""" psd_frequencies_data["freq"] = [float(v) for v in data["freq"]]
		psd_frequencies_data["power"] = [float(v) for v in data["power"]]
		for i in range(len(data["frequency_band_index"])):
			psd_frequencies_data["frequency_band_index"] = [bool(v) for v in data["frequency_band_index"][i]]
		psd_frequencies_data["labels"] = data["labels"] """
		#psd_frequencies_data["freq"] = freq_list
		#psd_frequencies_data["power"] = power_list
		#psd_frequencies_data["labels"] = label_list
		psd_frequencies_data["start_timestamp"] = row["start_timestamp"]
		psd_frequencies_data["end_timestamp"] = row["end_timestamp"]
		psd_frequencies_data["subject_uuid"] = row["subject_uuid"]
		psd_frequencies_data["video"] = row["video"]
		psd_frequencies_data_df = spark.createDataFrame(data=[psd_frequencies_data], schema=refined_psd_frequencies_data_schema)
		# --------------------

		# TODO: Create a data frame with the nk.hrv results? 

		# Store results into a delta table
		measures_df.write.format("delta").mode("append").save("s3a://trusted-zone/trusted_heartpy_measures")
		working_data_df.write.format("delta").mode("append").save("s3a://trusted-zone/trusted_working_data")
		breathing_data_df.write.format("delta").mode("append").save("s3a://refined-zone/refined_breathing_data")
		hr_data_df.write.format("delta").mode("append").save("s3a://refined-zone/refined_hr_data")
		rri_histogram_data_df.write.format("delta").mode("append").save("s3a://refined-zone/refined_rri_histogram_data")
		psd_frequencies_data_df.write.format("delta").mode("append").save("s3a://refined-zone/refined_psd_frequencies_data")

filtered_bvp_signal_stream_changes_df.writeStream.foreachBatch(process_batch).start()



#####################################################################################
## Infinite Loop waiting for new files (videos) to arrive at the raw bucket
#####################################################################################

def process_new_obj(obj):
    print(f"New file: {obj.object_name}")

    # TODO: Encrypt file (cant be done with snakebite, because the file contents cannot be read)
    # ...

    subject_uuid, extension = str(obj.object_name).split(".")
    filename_date = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
    new_filename = subject_uuid+"_"+filename_date+"."+extension

    client.copy_object(
        RAW_BUCKET_NAME,
        new_filename,
        CopySource(LANDING_BUCKET_NAME, str(obj.object_name)),
    ) 
    client.remove_object(LANDING_BUCKET_NAME, str(obj.object_name))
    print(f"File processed: {obj.object_name}")

    return new_filename

try:
    while True:
        # List all objects
        objects = client.list_objects(LANDING_BUCKET_NAME)

        for obj in objects:
            file_extension = str(obj.object_name).split(".")[-1]

            if file_extension in ["avi", "mov", "mp4"]:
                new_filename = process_new_obj(obj)
                process_video(new_filename)
            
        # Wait for new events
        time.sleep(5)
except KeyboardInterrupt:
    # Stop watching on keyboard interrupt
    spark.stop()
    pass