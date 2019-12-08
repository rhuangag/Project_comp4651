# Databricks notebook source
train_df = spark.read.format('csv').options(header='true', inferSchema='true').load('/FileStore/tables/train.csv')

# COMMAND ----------

building_metadata_df = spark.read.format('csv').options(header='true', inferSchema='true').load('/FileStore/tables/building_metadata.csv')

# COMMAND ----------

weather_train_df = spark.read.format('csv').options(header='true', inferSchema='true').load('/FileStore/tables/weather_train.csv')

# COMMAND ----------

####can skip this part

# COMMAND ----------

print(train_df.count())
print(train_df.na.drop().count())

# COMMAND ----------

building_metadata_df.count()

# COMMAND ----------

print(building_metadata_df.count())
print(building_metadata_df.na.drop().count())

# COMMAND ----------

weather_train_df.count()

# COMMAND ----------

print(weather_train_df.count())
print(weather_train_df.na.drop().count())

# COMMAND ----------

weather_train_df.groupBy("cloud_coverage").count().orderBy("count", ascending = False).show()

# COMMAND ----------

weather_train_df.groupBy("site_id").count().orderBy("count", ascending = False).show()

# COMMAND ----------

weather_train_df.groupBy("timestamp").count().orderBy("count", ascending = False).show()

# COMMAND ----------

weather_train_df.groupBy("air_temperature").count().orderBy("count", ascending = False).show()

# COMMAND ----------

weather_train_df.groupBy("dew_temperature").count().orderBy("count", ascending = False).show()

# COMMAND ----------

weather_train_df.groupBy("precip_depth_1_hr").count().orderBy("count", ascending = False).show()

# COMMAND ----------

weather_train_df.groupBy("sea_level_pressure").count().orderBy("count", ascending = False).show()

# COMMAND ----------

weather_train_df.groupBy("wind_direction").count().orderBy("count", ascending = False).show()

# COMMAND ----------

weather_train_df.groupBy("wind_speed").count().orderBy("count", ascending = False).show()

# COMMAND ----------

######skip above, run below

# COMMAND ----------

from pyspark.sql.functions import avg, col, when
from pyspark.sql.window import Window

# COMMAND ----------

w = Window().partitionBy('site_id')

#Replace negative values of 'qty' with Null, as we don't want to consider them while averaging.
#weather_train_df = weather_train_df.withColumn('qty',when(col('qty')<0,None).otherwise(col('qty')))
weather_train_df = weather_train_df.withColumn('cloud_coverage',when(col('cloud_coverage').isNull(),avg(col('cloud_coverage')).over(w)).otherwise(col('cloud_coverage')))
weather_train_df.show()

# COMMAND ----------

import pyspark.sql.functions as func

# COMMAND ----------

weather_train_df = weather_train_df.withColumn("cloud_coverage", func.round(weather_train_df["cloud_coverage"]).cast('integer'))

# COMMAND ----------

weather_train_df.groupBy("cloud_coverage").count().orderBy("count", ascending = False).show()

# COMMAND ----------

w = Window().partitionBy('site_id')

#Replace negative values of 'qty' with Null, as we don't want to consider them while averaging.
#weather_train_df = weather_train_df.withColumn('qty',when(col('qty')<0,None).otherwise(col('qty')))
weather_train_df = weather_train_df.withColumn('wind_direction',when(col('wind_direction').isNull(),avg(col('wind_direction')).over(w)).otherwise(col('wind_direction')))
weather_train_df = weather_train_df.withColumn("wind_direction", func.round(weather_train_df["wind_direction"]).cast('integer'))
weather_train_df.groupBy("wind_direction").count().orderBy("count", ascending = False).show()

# COMMAND ----------

weather_train_df = weather_train_df.withColumn('sea_level_pressure',when(col('sea_level_pressure').isNull(),avg(col('sea_level_pressure')).over(w)).otherwise(col('sea_level_pressure')))
weather_train_df.groupBy("sea_level_pressure").count().orderBy("count", ascending = False).show()


# COMMAND ----------

weather_train_df = weather_train_df.withColumn('precip_depth_1_hr',when(col('precip_depth_1_hr').isNull(),avg(col('precip_depth_1_hr')).over(w)).otherwise(col('precip_depth_1_hr')))
weather_train_df = weather_train_df.withColumn("precip_depth_1_hr", func.round(weather_train_df["precip_depth_1_hr"]).cast('integer'))
weather_train_df.groupBy("precip_depth_1_hr").count().orderBy("count", ascending = False).show()

# COMMAND ----------

weather_train_df=weather_train_df.na.drop()

# COMMAND ----------

w = Window().partitionBy('primary_use')
building_metadata_df = building_metadata_df.withColumn('year_built',when(col('year_built').isNull(),avg(col('year_built')).over(w)).otherwise(col('year_built')))
building_metadata_df = building_metadata_df.withColumn("year_built", func.round(building_metadata_df["year_built"]).cast('integer'))
building_metadata_df.groupBy("year_built").count().orderBy("count", ascending = False).show()

# COMMAND ----------

building_metadata_df = building_metadata_df.withColumn('floor_count',when(col('floor_count').isNull(),avg(col('floor_count')).over(w)).otherwise(col('floor_count')))
building_metadata_df = building_metadata_df.withColumn("floor_count", func.round(building_metadata_df["floor_count"]).cast('integer'))
building_metadata_df.groupBy("floor_count").count().orderBy("count", ascending = False).show()

# COMMAND ----------

building_metadata_df=building_metadata_df.na.drop()

# COMMAND ----------

building_metadata_df = building_metadata_df.withColumn("primary_use", when(col("primary_use")=="Education", 1)
                                   .when(col("primary_use")=="Office", 2)
                                   .when(col("primary_use")=="Entertainment/public assembly", 3)
                                   .when(col("primary_use")=="Public services", 4)
                                   .when(col("primary_use")=="Lodging/residential", 5)
                                   .when(col("primary_use")=="Other", 6)
                                   .when(col("primary_use")=="Healthcare", 7)
                                   .when(col("primary_use")=="Parking", 8)
                                   .when(col("primary_use")=="Warehouse/storage", 9)
                                   .when(col("primary_use")=="Manufacturing/industrial", 10)
                                   .when(col("primary_use")=="Retail", 11)
                                   .when(col("primary_use")=="Services", 12)
                                   .when(col("primary_use")=="Technology/science", 13)
                                   .when(col("primary_use")=="Food sales and service", 14)
                                   .when(col("primary_use")=="Utility", 15)
                                   .when(col("primary_use")=="Religious worship", 16))
building_metadata_df.groupBy("primary_use").count().orderBy("count", ascending = False).show()

# COMMAND ----------

train_df = train_df.withColumn("meter_reading", when(col("meter")==0, col("meter_reading")*0.293))

# COMMAND ----------

#####data processing ends here

# COMMAND ----------

meta_train_df =  building_metadata_df.join(train_df, (building_metadata_df['building_id'] == train_df['building_id']))
cond = [weather_train_df.site_id == meta_train_df.site_id, weather_train_df.timestamp == meta_train_df.timestamp]
trainDF =  weather_train_df.join(meta_train_df, cond)

# COMMAND ----------

datasetDF = trainDF.drop("timestamp", "site_id", "building_id")
datasetDF = datasetDF.na.fill(0)

# COMMAND ----------

# ***** vectorizer MODEL ****
from pyspark.ml.feature import VectorAssembler

vectorizer = VectorAssembler()
vectorizer.setInputCols(["air_temperature", "cloud_coverage", "dew_temperature", "precip_depth_1_hr", "sea_level_pressure", 
                         "wind_direction", "wind_speed", "square_feet", "year_built", "floor_count", "meter"])
vectorizer.setOutputCol("features")

# COMMAND ----------

split15DF, split85DF = datasetDF.randomSplit([15., 85.], seed=190)

# Let's cache these datasets for performance
testSetDF = split15DF#.cache()
trainingSetDF = split85DF#.cache()

# COMMAND ----------

# ***** LINEAR REGRESSION MODEL ****

from pyspark.ml.regression import LinearRegression
from pyspark.ml.regression import LinearRegressionModel
from pyspark.ml import Pipeline

# Let's initialize our linear regression learner
lr = LinearRegression()

# COMMAND ----------

# Now we set the parameters for the method
lr.setPredictionCol("predicted_meter_reading")\
  .setLabelCol("meter_reading")\
  .setMaxIter(100)\
  .setRegParam(0.15)


# We will use the new spark.ml pipeline API. If you have worked with scikit-learn this will be very familiar.
lrPipeline = Pipeline()

lrPipeline.setStages([vectorizer, lr])

# Let's first train on the entire dataset to see what we get
lrModel = lrPipeline.fit(trainingSetDF)


# COMMAND ----------

# The intercept is as follows:
intercept = lrModel.stages[1].intercept

# The coefficents (i.e., weights) are as follows:
weights = lrModel.stages[1].coefficients

# Create a list of the column names (without PE)
featuresNoLabel = [col for col in datasetDF.columns if col != "meter_reading"]

# Merge the weights and labels
coefficents = zip(weights, featuresNoLabel)

# Now let's sort the coefficients from greatest absolute weight most to the least absolute weight

equation = "y = {intercept}".format(intercept=intercept)
variables = []
for x in coefficents:
    weight = abs(x[0])
    name = x[1]
    symbol = "+" if (x[0] > 0) else "-"
    equation += (" {} ({} * {})".format(symbol, weight, name))

# Finally here is our equation
print("Linear Regression Equation: " + equation)


# COMMAND ----------

resultsDF = lrModel.transform(testSetDF)#.select("AT", "V", "AP", "RH", "PE", "Prediction_PE")

# COMMAND ----------

# t = resultsDF.groupBy().agg({'predicted_meter_reading': "mean"})
from pyspark.sql.functions import *
# predictionCol = "predicted_meter_reading"
# labelCol = "meter_reading"
# dataset = resultsDF
# dataset = dataset.withColumn('result_'+predictionCol, log(col(predictionCol)+1))
# dataset = dataset.withColumn('result_'+labelCol, log(col(labelCol)+1))
# dataset = dataset.withColumn('result', (col('result_'+predictionCol) - col('result_'+labelCol)))
# result = dataset.agg(avg(col("result")))
# result = result.collect()[0]["avg(result)"]
# print(result)

# COMMAND ----------



# Now let's compute an evaluation metric for our test dataset
from pyspark.ml.evaluation import Evaluator, RegressionEvaluator
from math import sqrt
from statistics import mean

class RMSLEEvaluator(Evaluator):

    def __init__(self, predictionCol="prediction", labelCol="label"):
        self.predictionCol = predictionCol
        self.labelCol = labelCol

    def _evaluate(self, dataset):
        """
        Returns a random number. 
        Implement here the true metric
        """
        new_dataset = dataset.withColumn('result_'+self.predictionCol, log(col(self.predictionCol)+1))
        new_dataset = new_dataset.withColumn('result_'+self.labelCol, log(col(self.labelCol)+1))
        new_dataset = new_dataset.withColumn('result', (col('result_'+self.predictionCol) - col('result_'+self.labelCol))**2)
        
        result = new_dataset.agg(avg(col("result")))
        result = result.collect()[0]["avg(result)"]
        return sqrt(result)
      
    def isLargerBetter(self):
        return True
# Create an RMSE evaluator using the label and predicted columns
regEval = RMSLEEvaluator(predictionCol="predicted_meter_reading", labelCol="meter_reading")

# Run the evaluator on the DataFrame
rmse = regEval.evaluate(resultsDF)

print("Root Mean Squared Error: %.2f" % rmse)

# COMMAND ----------


