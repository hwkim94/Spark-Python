# Spark
- spark을 공부하며 수행했던 실습파일들입니다.
- 연세대학교 빅데이터 학회 와이빅타 엔지니어링팀의 실습/과제/프로젝트를 위한 코드로도 이용됩니다.
- https://github.com/YBIGTA/EngineeringTeam

## 1. 예제 설명
### kafka-python
- project_2018-1
  - SparkSQL, SparkML을 이용해 데이터 분석하기
  - 주제 : expedia의 호텔 예약 예측
  - 데이터 : https://www.kaggle.com/c/expedia-hotel-recommendations
  
- face detection data mining
  - client : pyQT, openCV
    - webcam으로 부터 face detection정보(촬영 횟수, 얼굴 인식 횟수, 눈 인식 횟수, 사용자 인식 정도, 얼굴 bounding box의 coord 등)를 server로 http프로토콜을 이용하여 전송
    
  - server : nodejs, hadoop, spark, kafka
    - nodejs가 클라이언트로 부터 정보를 받고 kafka로 다시 전송
    - spark structured streaming을 통해 집중도, 참여도를 실시간으로 판단
    - 집중도, 참여도를 계산하기 위해서 sparkSQL을 통해 계산된 간단한 통계값을 이용(data mining)
    - 단일노드에서 작동
    
  - 필요한 기능
    - 학습된 데이터를 서버로 전송 및 서버의 hdfs에 저장하는 기능(현재는 그냥 로컬에 저장) 
  
## 2. 환경
- jupyter notebook
- Python 3.6
- Hadoop 2.7
- Spark 2.3.0
