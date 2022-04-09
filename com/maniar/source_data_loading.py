#read data from mysql and write to AWS S3.
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
import yaml
import os.path
import com.utils.utilities as ut

if __name__ == '__main__':

    # Create the SparkSession
    spark = SparkSession \
        .builder \
        .appName("Read ingestion enterprise applications") \
        .getOrCreate()
    spark.sparkContext.setLogLevel('ERROR')

    current_dir = os.path.abspath(os.path.dirname(__file__))
    app_config_path = os.path.abspath(current_dir + "/../../" + "application.yml")
    app_secrets_path = os.path.abspath(current_dir + "/../../" + ".secrets")

    conf = open(app_config_path)
    app_conf = yaml.load(conf, Loader=yaml.FullLoader)
    secret = open(app_secrets_path)
    app_secret = yaml.load(secret, Loader=yaml.FullLoader)

    s3_conf = app_conf['s3_conf']
    datalake_path = 's3a://' + s3_conf['s3_bucket'] + '/' + s3_conf['datalake_path']

    source_list = app_conf['source_list']

    for src in source_list:
        src_conf = app_conf[src]

        if src == 'SB':
            print("\nReading data from MySQL DB using SparkSession.read.format(),")
            txnDF = ut.read_from_mysql(spark,
                                       app_secret["mysql_conf"]["hostname"],
                                       app_secret["mysql_conf"]["port"],
                                       app_secret["mysql_conf"]["database"],
                                       src_conf["mysql_conf"]["dbtable"],
                                       src_conf["mysql_conf"]["partition_column"],
                                       app_secret["mysql_conf"]["username"],
                                       app_secret["mysql_conf"]["password"]
                                       )

            txnDF = txnDF.withColumn('ins_dt', current_date())
            txnDF.show()

            ut.write_to_s3(txnDF, datalake_path, src)

        elif src == 'OL':

            ol_txn_df = ut.read_from_sftp(spark,
                                        app_secret["sftp_conf"]["hostname"],
                                        app_secret["sftp_conf"]["port"],
                                        app_secret["sftp_conf"]["username"],
                                        os.path.abspath(current_dir + "/../../" + app_secret["sftp_conf"]["pem"]),
                                        src_conf["sftp_conf"]["directory"] + "/receipts_delta_GBR_14_10_2017.csv")
            ol_txn_df = ol_txn_df.withColumn('ins_dt', current_date())
            ol_txn_df.show(5, False)

            ut.write_to_s3(ol_txn_df, datalake_path, src)

        elif src == 'ADDR':

            address_df=ut.read_from_mongoDB(spark,
                                          src_conf["mongodb_config"]["database"],
                                          src_conf["mongodb_config"]["collection"],
                                          app_secret["mongodb_config"]["uri"])

            address_df = address_df.withColumn('ins_dt', current_date())


            address_df = address_df.select(col("address.street").alias("street"),
                                         col("address.city").alias("city"),
                                         col("address.state").alias("state"), "consumer_id", "mobile-no", "ins_dt"
                                         )
            address_df.show()

            ut.write_to_s3(address_df, datalake_path, src)


        elif src == 'CP':

            cp_df=ut.read_csv_from_s3(spark, 's3a://' + src_conf['s3_conf']['s3_bucket'] + "/" + src_conf['filename'])
            cp_df=cp_df.withColumn('ins_dt', current_date())

            cp_df.show()
            ut.write_to_s3(cp_df, datalake_path, src)

# spark-submit --master yarn --packages "mysql:mysql-connector-java:8.0.15,com.springml:spark-sftp_2.11:1.1.1,org.mongodb.spark:mongo-spark-connector_2.11:2.4.1,org.apache.hadoop:hadoop-aws:2.7.4" com/maniar/source_data_loading.py
