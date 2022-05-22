from __future__ import print_function
from pyspark.context import SparkContext
from pyspark.sql.session import SparkSession
from pyspark.ml.linalg import Vectors
from pyspark.ml import Pipeline
from pyspark.ml.feature import VectorAssembler, StringIndexer, MinMaxScaler 
from pyspark.ml.classification import LogisticRegression, NaiveBayes, DecisionTreeClassifier, RandomForestClassifier
from pyspark.ml.evaluation import MulticlassClassificationEvaluator

sc = SparkContext()
spark = SparkSession(sc)

data = spark.read.format('bigquery').option('table','bdl2022labs:irisdata.irisdatatable').load()
    
data.createOrReplaceTempView("irisdata")
data.show()

strind = StringIndexer(inputCol = 'Species', outputCol = 'label').fit(data)

train_data,test_data = data.randomSplit([0.70,0.30])
vecasmb = VectorAssembler(inputCols = ['SepalLength','SepalWidth','PetalLength','PetalWidth'], outputCol = 'features')

mmscaler = MinMaxScaler(inputCol = 'features', outputCol = "scaled_features")

# Various machine learning algorithms
logistic_regression = LogisticRegression(maxIter = 75, regParam = 0.15, elasticNetParam = 0.4)
naive_bayes = NaiveBayes(smoothing=1.0, modelType = "multinomial")
decision_tree = DecisionTreeClassifier(maxDepth = 5, labelCol = "label")
random_forest = RandomForestClassifier(labelCol = "label", featuresCol = "features", numTrees = 15)

def model_eval(model, ML_Model = ''):	
	train_prediction = model.transform(train_data).select("prediction", "label")
	test_prediction = model.transform(test_data).select("prediction", "label")
	print(ML_Model)
	print("Accuracy for trainig set = ", MulticlassClassificationEvaluator(metricName = "accuracy").evaluate(train_prediction))
	print("Accuracy for test set = ", MulticlassClassificationEvaluator(metricName = "accuracy").evaluate(test_prediction))

pipe = Pipeline(stages = [strind, vecasmb, mmscaler, decision_tree])
model = pipe.fit(train_data)
model_eval(model,'Decision Tree Classifier')
	
pipe = Pipeline(stages = [strind, vecasmb, mmscaler, logistic_regression])
model = pipe.fit(train_data)
model_eval(model,'Logistic regression')

pipe = Pipeline(stages = [strind, vecasmb, mmscaler, naive_bayes])
model = pipe.fit(train_data)
model_eval(model,'Naive Bayes')

pipe = Pipeline(stages = [strind, vecasmb, random_forest])
model = pipe.fit(train_data)
model_eval(model,'Random Forest')