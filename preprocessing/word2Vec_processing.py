import json
from pymongo import MongoClient as mc

import pyspark
from pyspark.ml.feature import Word2Vec
from pyspark.sql import SparkSession
from pyspark.sql import Row
from pyspark.sql.types import *
from pyspark.ml.feature import Tokenizer
from pyspark.ml import Pipeline
from pyspark.sql.functions import col

client = mc('mongodb://10.100.54.129:27017')
db = client['PaperAPI']
collection = db['paper01']

# Spark 세션 초기화
print(pyspark.__version__)
spark = SparkSession.builder \
    .appName("Word2VecExample") \
    .config("spark.executor.memory", "4g") \
    .config("spark.driver.memory", "2g") \
    .getOrCreate()

#spark.sparkContext.setLogLevel("INFO")  # 로깅 수준을 INFO로 설정

fields = ["journal_name", "publisher_name", "pub_year", "article_categories", "author", "affiliation"]

schema = StructType([
    StructField("journal_name", StringType(), False),
    StructField("publisher_name", StringType(), False),
    StructField("pub_year", StringType(), False),
    StructField("article_categories", StringType(), False),
    StructField("author", StringType(), False),
    StructField("affiliation",StringType(), True)
])

def extract_authors(author_data):
    if isinstance(author_data, list):
        return [author.get('#text', '') if isinstance(author, dict) else author for author in author_data]
    elif isinstance(author_data, dict):
        return [author_data.get('#text', '')]
    elif isinstance(author_data, str):
        return [author_data]
    else:
        return []


def verify_all_fields(document_all_fields):
    for document_fields in document_all_fields:
        if not all(value for value in document_fields.values()):
            return None


def extract_data(document_data):
    extract_document_field = []
    journal_info = document_data.get('journalInfo', {})
    journal_name = journal_info.get('journal-name', '')
    publisher_name = journal_info.get('publisher-name', '')
    pub_year = journal_info.get('pub-year', '')
    article_info = document_data.get('articleInfo', {})
    article_categories = article_info.get('article-categories', '')
    if article_categories is None:
        return None
    author_group = article_info.get('author-group', {})
    if author_group:
        authors = author_group.get('author', '')
        if authors is not None:
            for author in extract_authors(authors):
                affiliation = extract_affiliation_name(author)
                author_name = split_author_name(author)
                #print(affiliation, author_name)
                extract_document_field.append({
                    "journal_name": journal_name,
                    "publisher_name": publisher_name,
                    "pub_year": pub_year,
                    "article_categories": article_categories,
                    "author": author_name,
                    "affilication" : affiliation
                })
        else:
            return None
    else:
        return None
    if not verify_all_fields(extract_document_field):
        return extract_document_field
    else:
        return None


def split_author_name(author):
    return author.split('(')[0].strip()

def extract_affiliation_name(author):
    start_index = author.find('(')
    end_index = author.find(')')
    if start_index != -1 and end_index != -1:
        university = author[start_index + 1:end_index]
        return university.strip()
    else:
        return None


# get_data_test = []
# for document in collection.find():
#     ex_data = extract_data(document)
#     if ex_data:
#         for document_data in ex_data:
#             get_data_test.append(document_data)
#     print(ex_data)

def main():
    extracted_data = []
    document_cnt = 0
    for document in collection.find():
        extract_document_data = extract_data(document)
        document_cnt += 1
        if extract_document_data:
            for document_data in extract_document_data:
                extracted_data.append(document_data)
    df = spark.createDataFrame([Row(**x) for x in extracted_data], schema=schema)
    
    # 널 값을 가진 행 제거
    df = df.na.drop(subset=fields)

    #Tokenize
    tokenizer_stages = [Tokenizer(inputCol=field, outputCol=f"{field}_token") for field in fields]
    pipeline = Pipeline(stages = tokenizer_stages)
    df_transformed = pipeline.fit(df).transform(df)


    # Word2Vec 모델 설정 및 학습
    tokenizer_stages2 = [Tokenizer(inputCol=field, outputCol=f"{field}_tokens") for field in fields]
    word2vec_stages = [Word2Vec(vectorSize=100, minCount=0, inputCol=f"{field}_tokens", outputCol=f"{field}_emb") for field in fields]

    stages = [stage for pair in zip(tokenizer_stages2, word2vec_stages) for stage in pair]
    pipeline = Pipeline(stages=stages)
    df_transformed = pipeline.fit(df).transform(df)
    
    emb_columns = [col for col in df_transformed.columns if col.endswith("_emb")]

    emb_df = df_transformed.select(emb_columns)

    emb_df.show()


if __name__ == "__main__":
    main()