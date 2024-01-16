import json
from pymongo import MongoClient as mc

import pyspark
from pyspark.ml.feature import Word2Vec
from pyspark.sql import SparkSession
from pyspark.sql import Row
from pyspark.sql.functions import udf
from pyspark.sql.types import *

client = mc('mongodb://10.100.54.129:27017/')
db = client['PaperAPI']
collection = db['paper01']

# Spark 세션 초기화
print(pyspark.__version__)
spark = SparkSession.builder \
    .appName("Word2Vec") \
    .getOrCreate()


fields = ["journal_name", "publisher_name", "pub_year", "article_categories", "author"]
schema = StructType([
    StructField("journal_name", StringType(), False),
    StructField("publisher_name", StringType(), False),
    StructField("pub_year", StringType(), False),
    StructField("article_categories", StringType(), False),
    StructField("author", StringType(), False)
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

    # extract journalInfo field
    journal_info = document_data.get('journalInfo', {})
    journal_name = journal_info.get('journal-name', '')
    publisher_name = journal_info.get('publisher-name', '')
    pub_year = journal_info.get('pub-year', '')

    # extract articleInfo field
    article_info = document_data.get('articleInfo', {})
    article_categories = article_info.get('article-categories', '')
    if article_categories is None:
        return None

    # extract author-group field in articleInfo field
    author_group = article_info.get('author-group', {})
    if author_group:
        authors = author_group.get('author', '')
        if authors is not None:
            for author in extract_authors(authors):
                extract_document_field.append({
                    "journal_name": journal_name,
                    "publisher_name": publisher_name,
                    "pub_year": pub_year,
                    "article_categories": article_categories,
                    "author": author
                })
        else:
            return None
    else:
        return None

    # Verify that all fields were extracted properly
    if not verify_all_fields(extract_document_field):
        return extract_document_field
    else:
        return None


def split_author_name(author):
    return author.split('(')[0].strip().split()


def main():
    # with open("author_data.txt", "w", encoding='utf-8') as f:
    extracted_data = []
    document_cnt = 0

    for document in collection.find():
        extract_document_data = extract_data(document)
        document_cnt += 1
        if extract_document_data:
            for document_data in extract_document_data:
                extracted_data.append(document_data)
                print(document_cnt, document_data)
                #data_string = json.dumps(extract_document_data, ensure_ascii=False)
                #f.write(data_string + '\n')

        split_author_name_udf = udf(split_author_name, ArrayType(StringType()))
        df = spark.createDataFrame(extracted_data, schema=schema)
        df = df.withColumn("author", split_author_name_udf(df["author"]))

        # Word2Vec 모델 설정 및 학습
        word2Vec = Word2Vec(vectorSize=100, minCount=0, inputCol="author", outputCol="author_emb")
        model = word2Vec.fit(df)

        # 임베딩 결과를 DataFrame에 추가
        result_df = model.transform(df)
        result_df.show()
        spark.stop()


if __name__ == "__main__":
    main()