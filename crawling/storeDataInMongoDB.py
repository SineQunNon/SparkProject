import json
import math

import requests.exceptions
import xmltodict
from pymongo import MongoClient

keywords = ['컴퓨터',
            '데이터',
            '인공지능',
            '기계',
            '전자',
            '건축',
            '요리',
            '음악',
            '영화',
            '정치',
            '법률',
            '네트워크',
            '그래프',
            '철학',
            '미술',
            '종교',
            '여행',
            '우주',
            '역사',
            '경제',
            '사회',
            '교육',
            '의학',
            '생물',
            '화학',
            '물리',
            '수학',
            '통계',
            '지리',
            '심리',
            '언어',
            '문학',
            '체육',
            '게임',
            '취미',
            '사진',
            '패션',
            '자동차',
            '농업',
            '공업',
            '수공업']


API_BASE_URL = 'https://open.kci.go.kr/po/openapi/openApiSearch.kci'
API_CODE = 'articleSearch'
API_KEY = '16767175'
API_DISPLAY_COUNT = 100

client = MongoClient('10.100.54.129', 27017)
db = client['PaperAPI']
mycollection = db['paper02']

def GenerateAPIUrl(title, page):
    return f"{API_BASE_URL}?apiCode={API_CODE}&key={API_KEY}&title={title}&displayCount={API_DISPLAY_COUNT}&page={page}"

def sendToMongoDB(records):
    try:
        print(records)

        for record in records:
            mycollection.insert_one(record)
    except Exception as e:
        print("Error occurred: ", str(e))

#response 파싱하는 함수
def ProcessApiResponse(apiResponse):
    content = apiResponse.content
    dictType = xmltodict.parse(content)
    outputData = dictType['MetaData']['outputData']
    recordList = outputData.get('record', [])

    sendToMongoDB(recordList)

try:
    titles = keywords

    for title in titles:
        response = requests.get(GenerateAPIUrl(title, 1))

        if response.status_code == 200:
            ProcessApiResponse(response)
            content = response.content
            dictType = xmltodict.parse(content)
            metadata = dictType['MetaData']
            totalItems = int(metadata['outputData']['result']['total'])
            max_pages = math.ceil(totalItems/API_DISPLAY_COUNT)
            for page in range(2, max_pages+1):
                response = requests.get(GenerateAPIUrl(title, page))
                if(response.status_code == 200):
                    ProcessApiResponse(response)
                else:
                    print(f"API request failed for page {page}")
                    break
        else:
            print("API request failed")
except requests.exceptions.RequestException as e:
    print("Error request")



# for data in keywords:
#     print(data)
#     mycollection.insert_one({'item' : data})
