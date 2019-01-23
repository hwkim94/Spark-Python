import collections
import time

from pyspark import StorageLevel
from pyspark.sql import Row
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql import functions
from pyspark.sql.types import *
from pyspark.sql.window import Window


spark = SparkSession.builder.appName("project").getOrCreate()
train_data = spark.read.csv("s3://ybigta-spark-180602/data/train.csv")
test_data = spark.read.csv("s3://ybigta-spark-180602/data/test.csv")


def making_dataframe(df) :
    col_name = df.columns
    real_name = df.take(1)
    
    new_col_name = []
    for idx in range(len(col_name)) :
        new_col_name.append(real_name[0][idx])
        
    command = []
    for idx in range(len(col_name)) :
        command.append(col(col_name[idx]).alias(new_col_name[idx]))
        
    df = df.select(command)
    df = df.filter(df[real_name[0][0]] != real_name[0][0])
    
    return df

def is_booking(df) :
    new_df = df.where(df.is_booking == 1)
    new_df = new_df.drop("is_booking").select("*")
    
    return new_df

def drop_na(df) :
    new_df = df.na.drop()
    
    return new_df

def fill_na(df, ctg) :
    if ctg == "train" :
        new_df = df.na.fill({"date_time":"2013.5-08-00 00:00:00", "srch_ci":"2013.5-09-00","srch_co":"2013.5-09-00"})
    else :
        new_df = df.na.fill({"date_time":"2015-08-00 00:00:00", "srch_ci":"2015-09-00","srch_co":"2015-09-00"})
        
    return new_df

def string_to_double(df) :
    col_name = df.columns
    for name in col_name :
        df = df.withColumn(name, df[name].cast(DoubleType()))
            
    return df

def string_to_double2(df) :
    col_name = ["reserv_year", "reserv_month", "check_in_year", "check_in_month", "check_out_year", "check_out_month"]
    for name in col_name :
        df = df.withColumn(name, test_df[name].cast(DoubleType()))
            
    return df

def string_to_date(df, stringlist=["date_time", "srch_ci", "srch_co"]) :
    col1 = functions.udf(lambda date_time: date_time.split()[0].split("-")[0])
    col2 = functions.udf(lambda date_time: date_time.split()[0].split("-")[1])
    ##col3 = functions.udf(lambda date_time: date_time.split()[0].split("-")[2])   
    ##col4 = functions.udf(lambda date_time: date_time.split()[1].split(":")[0])
    
    new_df = df.select("*", col1(df.date_time).alias("reserv_year"), col2(df.date_time).alias("reserv_month"))
    
    col5 = functions.udf(lambda srch_ci: srch_ci.split("-")[0])
    col6 = functions.udf(lambda srch_ci: srch_ci.split("-")[1])
    #col7 = functions.udf(lambda srch_ci: srch_ci.split("-")[2])   
    
    new_df = new_df.select("*", col5(df.srch_ci).alias("check_in_year"), col6(df.srch_ci).alias("check_in_month"))
    
    col8 = functions.udf(lambda srch_co: srch_co.split("-")[0])
    col9 = functions.udf(lambda srch_co: srch_co.split("-")[1])
    #col10 = functions.udf(lambda srch_co: srch_co.split("-")[2])   
    
    new_df = new_df.select("*", col8(df.srch_co).alias("check_out_year"), col9(df.srch_co).alias("check_out_month"))
    new_df = new_df.drop(stringlist[0]).drop(stringlist[1]).drop(stringlist[2])
    
    return new_df

def fill_na_as_mean_or_most(df, ctg) :
    col_name = df.columns
    new_df = df.select("*")
    
    for col in col_name :
        print(col)
        try :
            col_avg = new_df.agg({col : "mean"}).collect()[0][0]
            new_df = new_df.na.fill({col :col_avg})
        except :
            if col == "check_in_year" and ctg=="test": 
                new_df = new_df.na.fill({col : 2015})
            elif col == "check_out_year" and ctg=="test" : 
                new_df = new_df.na.fill({col : 2015})
            elif col == "check_in_year" and ctg=="train": 
                new_df = new_df.na.fill({col : 2013.5})
            elif col == "check_out_year" and ctg=="train" : 
                new_df = new_df.na.fill({col : 2013.5})
            elif col == "check_in_month" : 
                new_df = new_df.na.fill({col : 9})
            elif col == "check_out_month" : 
                new_df = new_df.na.fill({col : 9})
            else :
                new_df = new_df.na.fill({col : 0})
                print("error : ", col)
            
    return new_df

def interval(df) :
    print("check")
    term1 = (df.check_in_year-df.reserv_year)*365 + (df.check_in_month-df.reserv_month)*30
    new_df = df.select("*", term1.alias("reserv_check_in_interval"))
    
    term2 = (df.check_out_year-df.check_in_year)*365 + (df.check_out_month-df.check_in_month)*30
    new_df2 = new_df.select("*", term2.alias("check_in_out_interval"))
    
    new_df3 = new_df2.drop("reserv_year")
    new_df3 = new_df3.drop("check_in_year")
    new_df3 = new_df3.drop("check_out_year")
    
    return new_df3

def drop_df(df, lst = ["posa_continent", "user_id", "cnt"]) :
    print("check")
    if len(lst) == 3 :
        new_df = df.drop(lst[0], lst[1], lst[2]) 
    else :
        new_df = df.drop(lst[0], lst[1])
    
    return new_df

def total_people(df) :
    print("check")
    total = df.srch_adults_cnt + df.srch_children_cnt
    avg_room_cnt = total / df.srch_rm_cnt
    new_df = df.select("*", total.alias("total_cnt"), avg_room_cnt.alias("avg_room_cnt"))
    
    return new_df

def normalize(df, col_lst) :
    new_df = df.select("*")
    
    for col in col_lst :
        print(col)
        described = new_df.describe(col).select("*")
        mean = float(described.take(3)[1][1])
        stddev = float(described.take(3)[2][1])
        
        if stddev == 0 :
            pass
        
        else :
            new_df = new_df.select("*", ((new_df[col]-mean)/stddev).alias("normed_"+col))
            new_df = new_df.drop(col)
        
    return new_df


train_df = making_dataframe(train_data)
test_df = making_dataframe(test_data)


train_df = is_booking(train_df)


train_df1_1 = drop_na(train_df)
train_df1_2 = fill_na(train_df, "train")
test_df1 = fill_na(test_df, "test")


train_df2_1 = string_to_double(string_to_date(train_df1_1))
train_df2_2 = string_to_double(string_to_date(train_df1_2))
test_df2 = string_to_double(string_to_date(test_df1))


train_df3_1 = train_df2_1.select("*")

train_df3_2 = fill_na_as_mean_or_most(train_df2_2, "train")
train_df3_2 = train_df2_2.na.drop()

test_df3 = fill_na_as_mean_or_most(test_df2, "test")
test_df3 = test_df3.na.fill(0)


train_df4_1 = interval(total_people(drop_df(train_df3_1)))
train_df4_2 = interval(total_people(drop_df(train_df3_2)))
test_df4 = interval(total_people(drop_df(test_df3, lst = ["posa_continent", "user_id"])))


col_lst = ["orig_destination_distance","srch_adults_cnt","srch_children_cnt","srch_rm_cnt","total_cnt","avg_room_cnt"]

train_df5_1 = normalize(train_df4_1, col_lst)
train_df5_2 = normalize(train_df4_2, col_lst)
test_df5 = normalize(test_df4, col_lst)


## modeling

from pyspark.ml.classification import LogisticRegression
from pyspark.ml.classification import LogisticRegressionModel

from pyspark.ml.classification import DecisionTreeClassifier
from pyspark.ml.feature import StringIndexer, VectorIndexer
from pyspark.ml.evaluation import MulticlassClassificationEvaluator

from pyspark.ml.classification import RandomForestClassifier
from pyspark.ml.feature import IndexToString, StringIndexer, VectorIndexer
from pyspark.ml.evaluation import MulticlassClassificationEvaluator

from pyspark.ml.classification import GBTClassifier
from pyspark.ml.feature import StringIndexer, VectorIndexer
from pyspark.ml.evaluation import MulticlassClassificationEvaluator

from pyspark.ml.classification import MultilayerPerceptronClassifier
from pyspark.ml.evaluation import MulticlassClassificationEvaluator

from pyspark.ml.feature import VectorAssembler
from pyspark.ml.pipeline import Pipeline
from pyspark.ml.pipeline import PipelineModel

train1 = train_df3_1.drop('cnt', 'user_id')
train2 = train_df3_2.drop('cnt', 'user_id')
test1 = test_df3.select("*")

train3 = train_df5_1.select("*")
train4 = train_df5_2.select("*")
test2 = test_df5.select("*")

train_features1 = [x for x in train_df3_1.columns if x not in ['hotel_cluster', 'cnt', 'user_id']]
train_features1 = [x for x in train_features1 if x in test1.columns]
train_features2 = [x for x in train_df5_2.columns if x != 'hotel_cluster']
train_features2 = [x for x in train_features2 if x in test2.columns]

target = "hotel_cluster"


# assembler

assembler1 = VectorAssembler(inputCols=train_features1, outputCol="features")
assembler2 = VectorAssembler(inputCols=train_features2, outputCol="features")


# multinomial logistic

lr1 = LogisticRegression(maxIter=120, regParam=0.01, labelCol=target)
lr2 = LogisticRegression(maxIter=120, regParam=0.01, labelCol=target)
lr3 = LogisticRegression(maxIter=130, regParam=0.01, labelCol=target)
lr4 = LogisticRegression(maxIter=130, regParam=0.01, labelCol=target)

pipeline1 = Pipeline(stages=[assembler1, lr1])
pipeline2 = Pipeline(stages=[assembler1, lr2])
pipeline3 = Pipeline(stages=[assembler2, lr3])
pipeline4 = Pipeline(stages=[assembler2, lr4])

pipelineModel1_1 = pipeline1.fit(train1)
pipelineModel1_2 = pipeline2.fit(train2)
pipelineModel1_3 = pipeline3.fit(train3)
pipelineModel1_4 = pipeline4.fit(train4)

pipelineModel1_1 = pipelineModel1_1.transform(test1).select("id", "prediction")
pipelineModel1_2 = pipelineModel1_2.transform(test1).select("id", "prediction")
pipelineModel1_3 = pipelineModel1_3.transform(test2).select("id", "prediction")
pipelineModel1_4 = pipelineModel1_4.transform(test2).select("id", "prediction")

pipelineModel1_1.write.format("csv").save("s3://ybigta-spark-180602/model1_result1")
pipelineModel1_2.write.format("csv").save("s3://ybigta-spark-180602/model1_result2")
pipelineModel1_3.write.format("csv").save("s3://ybigta-spark-180602/model1_result3")
pipelineModel1_4.write.format("csv").save("s3://ybigta-spark-180602/model1_result4")


# decision tree

dt1 = DecisionTreeClassifier(labelCol=target)
dt2 = DecisionTreeClassifier(labelCol=target)
dt3 = DecisionTreeClassifier(labelCol=target)
dt4 = DecisionTreeClassifier(labelCol=target)

pipeline1 = Pipeline(stages=[assembler1, dt1])
pipeline2 = Pipeline(stages=[assembler1, dt2])
pipeline3 = Pipeline(stages=[assembler2, dt3])
pipeline4 = Pipeline(stages=[assembler2, dt4])

pipelineModel2_1 = pipeline1.fit(train1)
pipelineModel2_2 = pipeline2.fit(train2)
pipelineModel2_3 = pipeline3.fit(train3)
pipelineModel2_4 = pipeline4.fit(train4)

pipelineModel2_1 = pipelineModel2_1.transform(test1).select("id", "prediction")
pipelineModel2_2 = pipelineModel2_2.transform(test1).select("id", "prediction")
pipelineModel2_3 = pipelineModel2_3.transform(test2).select("id", "prediction")
pipelineModel2_4 = pipelineModel2_4.transform(test2).select("id", "prediction")

pipelineModel2_1.write.format("csv").save("s3://ybigta-spark-180602/model2_result1")
pipelineModel2_2.write.format("csv").save("s3://ybigta-spark-180602/model2_result2")
pipelineModel2_3.write.format("csv").save("s3://ybigta-spark-180602/model2_result3")
pipelineModel2_4.write.format("csv").save("s3://ybigta-spark-180602/model2_result4")


# random forest

rf1 = RandomForestClassifier(labelCol=target, numTrees=30)
rf2 = RandomForestClassifier(labelCol=target, numTrees=30)
rf3 = RandomForestClassifier(labelCol=target, numTrees=40)
rf4 = RandomForestClassifier(labelCol=target, numTrees=40)

pipeline1 = Pipeline(stages=[assembler1, rf1])
pipeline2 = Pipeline(stages=[assembler1, rf2])
pipeline3 = Pipeline(stages=[assembler2, rf3])
pipeline4 = Pipeline(stages=[assembler2, rf4])

pipelineModel3_1 = pipeline1.fit(train1)
pipelineModel3_2 = pipeline2.fit(train2)
pipelineModel3_3 = pipeline3.fit(train3)
pipelineModel3_4 = pipeline4.fit(train4)

pipelineModel3_1 = pipelineModel3_1.transform(test1).select("id", "prediction")
pipelineModel3_2 = pipelineModel3_2.transform(test1).select("id", "prediction")
pipelineModel3_3 = pipelineModel3_3.transform(test2).select("id", "prediction")
pipelineModel3_4 = pipelineModel3_4.transform(test2).select("id", "prediction")

pipelineModel3_1.write.format("csv").save("s3://ybigta-spark-180602/model3_result1")
pipelineModel3_2.write.format("csv").save("s3://ybigta-spark-180602/model3_result2")
pipelineModel3_3.write.format("csv").save("s3://ybigta-spark-180602/model3_result3")
pipelineModel3_4.write.format("csv").save("s3://ybigta-spark-180602/model3_result4")


# mlp

layers1 = [len(train_features1), 64, 128, 100]
layers2 = [len(train_features2), 64, 128, 100]

mlp1 = MultilayerPerceptronClassifier(layers = layers1, labelCol=target, maxIter=150, blockSize=128)
mlp2 = MultilayerPerceptronClassifier(layers = layers1, labelCol=target, maxIter=150, blockSize=128)
mlp3 = MultilayerPerceptronClassifier(layers = layers2, labelCol=target, maxIter=170, blockSize=128)
mlp4 = MultilayerPerceptronClassifier(layers = layers2, labelCol=target, maxIter=170, blockSize=128)

pipeline1 = Pipeline(stages=[assembler1, mlp1])
pipeline2 = Pipeline(stages=[assembler1, mlp2])
pipeline3 = Pipeline(stages=[assembler2, mlp3])
pipeline4 = Pipeline(stages=[assembler2, mlp4])

pipelineModel5_1 = pipeline1.fit(train1)
pipelineModel5_2 = pipeline2.fit(train2)
pipelineModel5_3 = pipeline3.fit(train3)
pipelineModel5_4 = pipeline4.fit(train4)

pipelineModel5_1 = pipelineModel5_1.transform(test1).select("id", "prediction")
pipelineModel5_2 = pipelineModel5_2.transform(test1).select("id", "prediction")
pipelineModel5_3 = pipelineModel5_3.transform(test2).select("id", "prediction")
pipelineModel5_4 = pipelineModel5_4.transform(test2).select("id", "prediction")

pipelineModel5_1.write.format("csv").save("s3://ybigta-spark-180602/model5_result1")
pipelineModel5_2.write.format("csv").save("s3://ybigta-spark-180602/model5_result2")
pipelineModel5_3.write.format("csv").save("s3://ybigta-spark-180602/model5_result3")
pipelineModel5_4.write.format("csv").save("s3://ybigta-spark-180602/model5_result4")
