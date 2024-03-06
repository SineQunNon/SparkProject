from pyspark.ml import Pipeline
from pyspark.ml.feature import Word2Vec, VectorAssembler, Tokenizer, PCA
from pyspark.ml.clustering import PowerIterationClustering
from pyspark.ml.clustering import KMeans
from pyspark.ml.evaluation import ClusteringEvaluator
from pyspark.ml.linalg import Vectors
from pyspark.sql import SparkSession,Row
from pyspark.sql.types import *
from pyspark.sql.functions import col, udf

client = mc('mongodb://10.100.54.129:27017')
db = client['PaperAPI']
collection = db['extracted_data']

df = spark.createDataFrame([Row(**x) for x in extracted_data], schema=schema)
# 널 값을 가진 행 제거
df = df.na.drop(subset=fields)
#Tokenize
tokenizer_stages = [Tokenizer(inputCol=field, outputCol=f"{field}_token") for field in fields]
pipeline = Pipeline(stages = tokenizer_stages)
df_transformed = pipeline.fit(df).transform(df)
# Word2Vec 모델 설정 및 학습
tokenizer_stages2 = [Tokenizer(inputCol=field, outputCol=f"{field}_tokens") for field in fields]
word2vec_stages = [Word2Vec(vectorSize=128, minCount=0, inputCol=f"{field}_tokens", outputCol=f"{field}_emb") for field in fields]
stages = [stage for pair in zip(tokenizer_stages2, word2vec_stages) for stage in pair]
pipeline = Pipeline(stages=stages)
df_transformed = pipeline.fit(df).transform(df)
emb_columns = [col for col in df_transformed.columns if col.endswith("_emb")]
emb_df = df_transformed.select(emb_columns)
emb_df.show()

#vector_assembler_stages = [VectorAssembler(inputCols=[f"{field}_emb"], outputCol=f"{field}_vec") for field in fields]
#vector_assembler_pipeline = Pipeline(stages=vector_assembler_stages)
#df_final = vector_assembler_pipeline.fit(emb_df).transform(emb_df)
#vec_columns = [ col for col in df_final.columns if col.endswith("_vec")]
#df_features = df_final.select(vec_columns)
#df_features.show()

vecAssembler = VectorAssembler(outputCol="features")
fields_emb = ["journal_name_emb", "publisher_name_emb", "pub_year_emb", "article_categories_emb", "author_emb", "affiliation_emb"]
vecAssembler.setInputCols(fields_emb)
emb_features_df = vecAssembler.transform(emb_df).select("features")

#PCA processing
pca = PCA(k=3, inputCol = "features")
pca.setOutputCol("pca_features")

pca_result = pca.fit(emb_features_df).transform(emb_features_df).select("pca_features")

#Kmeans processing
kmeans = KMeans().setK(4).setSeed(1).setFeaturesCol("pca_features")
kmeans_model = kmeans.fit(pca_result)

result_transformed = kmeans_model.transform(pca_result).select("pca_features", "prediction")
result_transformed.show()

#predictions = kmeans_model.transform(pca_result)
centers = kmeans_model.clusterCenters()
print(centers)
#vector_to_list_udf = udf(lambda x : list(x.toArray()), DoubleType())
#cluster 환경에서는 udf 적용이 안되나?

#udf등록
#udf_vector_to_list = udf(vector_to_list_udf, ArrayType(DoubleType()))

#emb_df_with_list = emb_features_df.withColumn("features_list", vector_to_list_udf("features"))
#pic = PowerIterationClustering(k=3, maxIter=20, initMode= "degree", weightCol="features_list")
#pic.assignClusters(emb_df).show()

# def vector_to_list_udf(vector):
#     return vector.toArray().tolist()

#get_first_element = udf(lambda v: float(v[0]), DoubleType())
#emb_df = emb_df.withColumn("author_emb_numeric", get_first_element("author_emb"))
