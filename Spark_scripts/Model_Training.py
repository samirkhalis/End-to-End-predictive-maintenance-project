# from pyspark.sql import SparkSession
# from pyspark.ml import Pipeline
# from pyspark.ml.feature import VectorAssembler, StringIndexer, OneHotEncoder, StandardScaler
# from pyspark.ml.classification import RandomForestClassifier
# from pyspark.ml.evaluation import MulticlassClassificationEvaluator
# from pyspark.sql.functions import col, count

# # Initialize Spark session
# spark = SparkSession.builder.appName('MulticlassClassification').getOrCreate()

# # Load the dataset into a Spark DataFrame
# df = spark.read.csv('/opt/spark/predictive_maintenance.csv', header=True, inferSchema=True)

# # Define features and target column
# num_features = ["Air temperature [K]", "Process temperature [K]", "Rotational speed [rpm]", "Torque [Nm]", "Tool wear [min]"]
# cat_features = ["Type"]
# target_variable = 'Failure Type'

# # Preprocessing for numerical features
# num_assembler = VectorAssembler(inputCols=num_features, outputCol='num_features')
# num_scaler = StandardScaler(inputCol='num_features', outputCol='scaled_num_features')

# # Preprocessing for categorical features
# cat_indexer = StringIndexer(inputCol='Type', outputCol='Type_index')
# cat_encoder = OneHotEncoder(inputCol='Type_index', outputCol='Type_encoded')

# # Indexing target variable for classification
# target_indexer = StringIndexer(inputCol=target_variable, outputCol='target_index')

# # VectorAssembler to combine features into a single vector
# feature_assembler = VectorAssembler(inputCols=num_features + ['Type_encoded'], outputCol='features')

# # Split the data into train and test sets
# train_df, test_df = df.randomSplit([0.8, 0.2], seed=42)

# # Create a pipeline to apply the transformations
# pipeline = Pipeline(stages=[
#     target_indexer,
#     num_assembler,
#     num_scaler,
#     cat_indexer,
#     cat_encoder,
#     feature_assembler
# ])

# # Fit the pipeline to the training data
# train_df_transformed = pipeline.fit(train_df).transform(train_df)
# test_df_transformed = pipeline.fit(train_df).transform(test_df)

# # Initialize the RandomForestClassifier with your specified hyperparameters
# rf_classifier = RandomForestClassifier(
#     labelCol='target_index', 
#     featuresCol='features', 
#     numTrees=196,             # Number of trees (equivalent to n_estimators)
#     maxDepth=10,              # Maximum depth (equivalent to max_depth)
#     minInstancesPerNode=2,    # Minimum samples required to split a node (equivalent to min_samples_split)
#     minInfoGain=0.0,          # This is an approximation to min_samples_leaf (not directly available)
#     seed=42                   # Random seed for reproducibility (changed from randomSeed)
# )

# # Train the RandomForest model
# rf_model = rf_classifier.fit(train_df_transformed)

# # Make predictions on the test set
# predictions = rf_model.transform(test_df_transformed)

# # Evaluate the model using F1 score
# evaluator = MulticlassClassificationEvaluator(
#     labelCol='target_index', 
#     predictionCol='prediction', 
#     metricName='f1'
# )

# # Calculate the weighted F1 score
# f1 = evaluator.evaluate(predictions)
# print(f"F1 Score on Test Set (Weighted): {f1}")

# # Additional Evaluation Metrics
# accuracy = evaluator.setMetricName('accuracy').evaluate(predictions)
# print(f"Accuracy on Test Set: {accuracy}")

# # Confusion Matrix
# predictions.groupBy('target_index', 'prediction').count().show()


import logging
from pyspark.sql import SparkSession
from pyspark.ml import Pipeline
from pyspark.ml.feature import VectorAssembler, StringIndexer, OneHotEncoder, StandardScaler
from pyspark.ml.classification import RandomForestClassifier
from pyspark.ml.evaluation import MulticlassClassificationEvaluator
from pyspark.sql.functions import col, count

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Initialize Spark session
spark = SparkSession.builder.appName('RandomForestFailurePrediction&Classification').getOrCreate()

# Log the start of the session
logger.info("Spark session started.")

# Load the dataset into a Spark DataFrame
logger.info("Loading dataset from /opt/spark/predictive_maintenance.csv...")
df = spark.read.csv('/opt/spark/predictive_maintenance.csv', header=True, inferSchema=True)
logger.info(f"Dataset loaded with {df.count()} rows and {len(df.columns)} columns.")

# Define features and target column
num_features = ["Air temperature [K]", "Process temperature [K]", "Rotational speed [rpm]", "Torque [Nm]", "Tool wear [min]"]
cat_features = ["Type"]
target_variable = 'Failure Type'

# Preprocessing for numerical features
logger.info("Setting up feature preprocessing pipeline...")
num_assembler = VectorAssembler(inputCols=num_features, outputCol='num_features')
num_scaler = StandardScaler(inputCol='num_features', outputCol='scaled_num_features')

# Preprocessing for categorical features
cat_indexer = StringIndexer(inputCol='Type', outputCol='Type_index')
cat_encoder = OneHotEncoder(inputCol='Type_index', outputCol='Type_encoded')

# Indexing target variable for classification
target_indexer = StringIndexer(inputCol=target_variable, outputCol='target_index')

# VectorAssembler to combine features into a single vector
feature_assembler = VectorAssembler(inputCols=num_features + ['Type_encoded'], outputCol='features')

# Split the data into train and test sets
train_df, test_df = df.randomSplit([0.8, 0.2], seed=42)
logger.info(f"Data split into training ({train_df.count()} rows) and testing ({test_df.count()} rows) sets.")

# Create a pipeline to apply the transformations
pipeline = Pipeline(stages=[
    target_indexer,
    num_assembler,
    num_scaler,
    cat_indexer,
    cat_encoder,
    feature_assembler
])

# Fit the pipeline to the training data
logger.info("Fitting the pipeline to the training data...")
train_df_transformed = pipeline.fit(train_df).transform(train_df)
test_df_transformed = pipeline.fit(train_df).transform(test_df)
logger.info("Pipeline transformation completed.")

# Initialize the RandomForestClassifier with your specified hyperparameters
logger.info("Initializing RandomForestClassifier...")
rf_classifier = RandomForestClassifier(
    labelCol='target_index',
    featuresCol='features',
    numTrees=196,             # Number of trees (equivalent to n_estimators)
    maxDepth=10,              # Maximum depth (equivalent to max_depth)
    minInstancesPerNode=2,    # Minimum samples required to split a node (equivalent to min_samples_split)
    minInfoGain=0.0,          # This is an approximation to min_samples_leaf (not directly available)
    seed=42                   # Random seed for reproducibility
)

# Train the RandomForest model
logger.info("Training the RandomForest model...")
rf_model = rf_classifier.fit(train_df_transformed)
logger.info("Model training completed.")

# Save the model to a file
import cloudpickle

model_save_path = '/tmp/rf_model.joblib'
with open(model_save_path, 'wb') as f:
    cloudpickle.dump(rf_model, f)
logger.info(f"Model saved to {model_save_path}.")


# Make predictions on the test set
logger.info("Making predictions on the test set...")
predictions = rf_model.transform(test_df_transformed)

# Evaluate the model using F1 score
evaluator = MulticlassClassificationEvaluator(
    labelCol='target_index',
    predictionCol='prediction',
    metricName='f1'
)

# Calculate the weighted F1 score
f1 = evaluator.evaluate(predictions)
logger.info(f"F1 Score on Test Set (Weighted): {f1}")

# Additional Evaluation Metrics
accuracy = evaluator.setMetricName('accuracy').evaluate(predictions)
logger.info(f"Accuracy on Test Set: {accuracy}")

# Confusion Matrix
logger.info("Displaying confusion matrix...")
predictions.groupBy('target_index', 'prediction').count().show()

# Stop the Spark session
spark.stop()
logger.info("Spark session stopped.")
