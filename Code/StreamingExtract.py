import json

from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *
from Transform import Gold
from dotenv import load_dotenv
import os
from datetime import date
import logging

logging.getLogger('py4j').setLevel(logging.ERROR)
load_dotenv()


class streamingGold:

    def __init__(self):
        self.spark = (SparkSession
                      .builder
                      .master('local[3]')
                      .appName('RealEstateStreaming')
                      .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.2,"
                                                     "org.postgresql:postgresql:42.2.5")
                      .getOrCreate())

        self.basedir = f"/Users/abhishekteli/Documents/Projects/RealEstate/checkpoint/"

        self.spark.sparkContext.setLogLevel("ERROR")

    def getSchema(self):
        return (StructType([
                    StructField('dateSold', DateType(), True),
                    StructField('propertyType', StringType(), True),
                    StructField('lotAreaValue', DoubleType(), True),
                    StructField('address', StringType(), True),
                    StructField('priceChange', DoubleType(), True),
                    StructField('zestimate', DoubleType(), True),
                    StructField('imgSrc', StringType(), True),
                    StructField('price', IntegerType(), True),
                    StructField('detailUrl', StringType(), True),
                    StructField('bedrooms', IntegerType(), True),
                    StructField('longitude', DoubleType(), True),
                    StructField('latitude', DoubleType(), True),
                    StructField('listingStatus', StringType(), True),
                    StructField('zpid', StringType(), True),
                    StructField('rentZestimate', DoubleType(), True),
                    StructField('daysOnZillow', IntegerType(), True),
                    StructField('bathrooms', DoubleType(), True),
                    StructField('livingArea', IntegerType(), True),
                    StructField('country', StringType(), True),
                    StructField('currency', StringType(), True),
                    StructField('lotAreaUnit', StringType(), True),
                    StructField('hasImage', BooleanType(), True)
                ])
        )

    def readData(self):
        return (self.spark.readStream
                .format('kafka')
                .option("kafka.bootstrap.servers", "localhost:9092")
                .option("subscribe", "realEstates")
                .option("startingOffsets", "earliest")
                .load()
                )

    def getRealEstateData(self, kafka_df):
        schema = self.getSchema()
        json_df = kafka_df.withColumn("json_array", from_json(col("value").cast("string"), ArrayType(schema)))
        exploded_df = json_df.withColumn("data", explode(col("json_array"))).select("data.*")
        return exploded_df

    def SaveToDatabase(self, resultDF, batch_id,):
        url = "jdbc:postgresql://localhost:5432/realestate"
        properties = {
            "user": os.getenv('USERNAME'),
            "password": os.getenv('DATABASE_PASSWORD'),
            "driver": "org.postgresql.Driver"
        }
        try:
            load_date_to_compare = resultDF.select('loadDate').first()[0]
            mode = 'overwrite' if load_date_to_compare == date.today() else 'append'
            resultDF.write.jdbc(url=url, table='streaminghouses', mode='append', properties=properties)
        except Exception as e:
            print('')

    def writeToDatabase(self, realEstateDF):
        sQuery = (realEstateDF.writeStream
                  .format('console')
                  .foreachBatch(self.SaveToDatabase)
                  .option("checkpointLocation", f"{self.basedir}")
                  .outputMode('update')
                  .start()
                  )
        return sQuery

    def processData(self):
        gd = Gold()
        kafka_df = self.readData()
        parsed_df = self.getRealEstateData(kafka_df)
        transformedDF = gd.transformData(parsed_df)
        sQuery = self.writeToDatabase(transformedDF)
        return sQuery


