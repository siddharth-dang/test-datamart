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
        .master('local[*]') \
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

    target_list = app_conf["target_list"]

    for tgt in target_list:
        tgt_conf = app_conf[tgt]
        src_list = tgt_conf["source_data"]

        for src_data in src_list:
            stg_df = spark.read \
                .parquet(datalake_path + "/" + src_data)
            stg_df.createOrReplaceTempView(src_data)


        if tgt == 'REGIS_DIM':

            regis_dim_df = spark.sql(tgt_conf["loadingQuery"])
            regis_dim_df.show()

            jdbc_url = ut.get_redshift_jdbc_url(app_secret)
            print(jdbc_url)

            ut.write_to_RedShift(regis_dim_df, jdbc_url, app_conf, tgt_conf)

        elif tgt == 'CHILD_DIM':

            child_dim_df = spark.sql(tgt_conf["loadingQuery"])
            child_dim_df.show()

            jdbc_url = ut.get_redshift_jdbc_url(app_secret)
            print(jdbc_url)

            ut.write_to_RedShift(child_dim_df, jdbc_url, app_conf, tgt_conf)


        elif tgt == 'RTL_TXN_FCT':

            jdbc_url = ut.get_redshift_jdbc_url(app_secret)
            print(jdbc_url)
            print("\nReading REGIS_DIM Data")
            dim_df = ut.read_from_RedShift(spark, jdbc_url, app_conf, tgt_conf["target_src_table"])
            dim_df.createOrReplaceTempView("REGIS_DIM")
            dim_df.show()

            rtl_txn_fct_df = spark.sql(tgt_conf["loadingQuery"])

            ut.write_to_RedShift(rtl_txn_fct_df, jdbc_url, app_conf, tgt_conf)



# spark-submit --jars "https://s3.amazonaws.com/redshift-downloads/drivers/jdbc/1.2.36.1060/RedshiftJDBC42-no-awssdk-1.2.36.1060.jar" --packages "org.apache.spark:spark-avro_2.11:2.4.2,io.github.spark-redshift-community:spark-redshift_2.11:4.0.1,org.apache.hadoop:hadoop-aws:2.7.4" com/maniar/target_data_loading.py
# --packages "org.apache.hadoop:hadoop-aws:2.7.4" com/maniar/target_data_loading.py
