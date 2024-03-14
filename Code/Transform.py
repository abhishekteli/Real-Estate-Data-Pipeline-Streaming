from pyspark.sql import SparkSession
from pyspark.sql.types import *
from datetime import date
from pyspark.sql.functions import *
import logging

logging.getLogger('py4j').setLevel(logging.ERROR)

class Gold:

    def __init__(self):
        self.basedir_load = (f"/Users/abhishekteli/Documents/Projects/RealEstate/rawData/"
                        f"year={date.today().year}/month={date.today().month}/day={date.today().day}/")

        self.spark = SparkSession \
                    .builder \
                    .appName("RealEstate") \
                    .config("spark.jars.packages", "org.postgresql:postgresql:42.2.5") \
                    .getOrCreate()

        self.spark.sparkContext.setLogLevel("ERROR")

    def getSchema(self):
        return (
            StructType([
                StructField('dateSold', DateType(), True),
                StructField('propertyType', StringType(), False),
                StructField('lotAreaValue', DoubleType(), False),
                StructField('address', StringType(), False),
                StructField('variableData', StructType([
                    StructField('text', StringType(), True),
                    StructField('type', StringType(), True)
                ]), True),
                StructField('priceChange', DoubleType(), True),
                StructField('zestimate', DoubleType(), True),
                StructField('imgSrc', StringType(), True),
                StructField('price', IntegerType(), False),
                StructField('detailUrl', StringType(), False),
                StructField('bedrooms', IntegerType(), True),
                StructField('contingentListingType', StringType(), True),
                StructField('longitude', DoubleType(), False),
                StructField('latitude', DoubleType(), False),
                StructField('listingStatus', StringType(), False),
                StructField('zpid', StringType(), False),
                StructField('listingSubType', StructType([
                    StructField('is_FSBA', BooleanType(), True),
                    StructField('is_openHouse', BooleanType(), True)
                ]), True),
                StructField('rentZestimate', DoubleType(), True),
                StructField('daysOnZillow', IntegerType(), False),
                StructField('bathrooms', DoubleType(), True),
                StructField('livingArea', IntegerType(), True),
                StructField('country', StringType(), False),
                StructField('currency', StringType(), False),
                StructField('lotAreaUnit', StringType(), False),
                StructField('hasImage', BooleanType(), False)
            ])
        )

    def getData(self):

        return (
            self.spark.read
            .format("json")
            .schema(self.getSchema())
            .load(f"{self.basedir_load}/")
            .drop('variableData', 'contingentListingType', 'listingSubType')
        )

    def transformData(self, realEstateDF):

        realEstateDF = realEstateDF.withColumn('street', split(col('address'),',')[0].cast('string')) \
                                    .withColumn('city', split(col('address'),',')[1].cast('string')) \
                                    .withColumn('state', split(split(col('address'),',')[2]," ")[1].cast("string"))\
                                    .withColumn('zip', split(split(col('address'),',')[2]," ")[2])

        realEstateDF = realEstateDF.fillna({'lotAreaValue': 0, 'priceChange': 0, 'zestimate': 0, 'rentZestimate': 0,
                                            'daysOnZillow': 1}) \
                                    .drop('address')

        realEstateDF = realEstateDF.withColumn('currentPrice', col('price') + col('priceChange')) \
                                    .withColumnRenamed('price', 'initialPrice') \
                                    .withColumnRenamed('rentZestimate', 'rent')

        realEstateDF = realEstateDF.withColumn('lotAreaValue', when(realEstateDF.lotAreaValue < 1, 0)\
                                               .otherwise(realEstateDF.lotAreaValue))

        # realEstateDF = realEstateDF.withColumn('street', when((col('state').isNull()) & (col('city').isNull()),
        #                                                       concat(col('street'), col('city')))\
        #                                        .otherwise(col('street'))) \
        #                             .withColumn('city', regexp_extract(input_file_name(),".*/(.+?),",1)) \
        #                             .withColumn('state', regexp_extract(input_file_name(), ",%20([a-z]{2})", 1)) \
        #                             .withColumn('city', regexp_replace(col('city'), "%20", ' '))

        realEstateDF = realEstateDF.withColumn("loadDate", current_date()).drop('variableData','imgSrc', 'detailUrl',
                                                                                'contingentListingType'
                                                                                'listingSubType')

        return realEstateDF

    def processData(self):
        rawDF = self.getData()
        rawDF.show()
        realEstateDF = self.transformData(rawDF)
        return realEstateDF

