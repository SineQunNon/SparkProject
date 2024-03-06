import json
from pymongo import MongoClient as mc

import pyspark


client = mc('mongodb://10.100.54.129:27017')
db = client['PaperAPI']
collection = db['paper01']

# Spark 세션 초기화
print(pyspark.__version__)
spark = SparkSession.builder \
    .appName("DataframeProcessing") \
    .getOrCreate()
    #.config("spark.executor.memory", "4g") \
    #.config("spark.driver.memory", "2g") \
    

#spark.sparkContext.setLogLevel("INFO")  # 로깅 수준을 INFO로 설정

fields = ["journal_name", "publisher_name", "pub_year", "article_categories", "article_title", "abstract", "author", "affiliation"]

schema = StructType([
    StructField("journal_name", StringType(), False),
    StructField("publisher_name", StringType(), False),
    StructField("pub_year", StringType(), False),
    StructField("article_categories", StringType(), False),
    StructField("article_title", StringType(), False),
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
    
def extract_korean_title(title_group):
    # title-group이 존재하지 않는 경우 빈 문자열 반환
    if 'article-title' not in title_group:
        return None
    article_title_data = title_group['article-title']
    # article-title이 리스트인 경우
    if isinstance(article_title_data, list):
        for title_info in article_title_data:
            lang_code = title_info.get('@lang', '')
            if lang_code == 'original':
                return title_info.get('#text', '')
    # article-title이 딕셔너리인 경우
    elif isinstance(article_title_data, dict):
        lang_code = article_title_data.get('@lang', '')
        if lang_code == 'original':
            return article_title_data.get('#text', '')
    return None

def extract_korean_abstract(abstract_group):
        if 'abstract' not in abstract_group:
            return None
        abstract_data = abstract_group['abstract']
        if isinstance(abstract_data, list):
            for abstract_info in abstract_data:
                lang_code = abstract_info.get('@lang', '')
                if lang_code == 'original':
                    return abstract_info.get('#text', '')
        elif isinstance(abstract_data, dict):
            lang_code = abstract_data.get("@lang", '')
            if lang_code == 'original':
                return abstract_data.get('$text', '')
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
    #print(author_group)
    title_group = article_info.get('title-group', {})
    article_title = extract_korean_title(title_group)
    if article_title is None:
        return None
    abstract_group = article_info.get('abstract-group', {})
    abstract = extract_korean_abstract(abstract_group)
    if abstract is None:
        return None
    #print(article_title)
    #article_title = title_group.get('article-title', [{}])[0].get('#text', '')
    if article_title is None:
        return None
    if author_group:
        authors = author_group.get('author', '')
        extract_document_field.append({
                    "journal_name": journal_name,
                    "publisher_name": publisher_name,
                    "pub_year": pub_year,
                    "article_categories": article_categories,
                    "article_title" : article_title,
                    "abstract" : abstract,
                    "author": authors,
                    # "affilication" : affiliation
                })
        # if authors is not None:
        #     for author in extract_authors(authors):
        #         affiliation = extract_affiliation_name(author)
        #         author_name = split_author_name(author)
        #         #print(affiliation, author_name)
        #         extract_document_field.append({
        #             "journal_name": journal_name,
        #             "publisher_name": publisher_name,
        #             "pub_year": pub_year,
        #             "article_categories": article_categories,
        #             "article_title" : article_title,
        #             "abstract" : abstract,
        #             "author": author_name,
        #             "affilication" : affiliation
        #         })
        # else:
        #     return None
    else:
        return None
    if not verify_all_fields(extract_document_field):
        return extract_document_field
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
    
if __name__ == "__main__":
    main()