## face detection data mining
- client : pyQT, openCV
  - webcam으로 부터 face detection정보(촬영 횟수, 얼굴 인식 횟수, 눈 인식 횟수, 사용자 인식 정도, 얼굴 bounding box의 coord 등)를 server로 http프로토콜을 이용하여 전송
    
- server : nodejs, hadoop, spark, kafka
    - nodejs가 클라이언트로 부터 정보를 받고 kafka로 다시 전송
    - spark structured streaming을 통해 집중도, 참여도를 실시간으로 판단
    - 집중도, 참여도를 계산하기 위해서 sparkSQL을 통해 계산된 간단한 통계값을 이용(data mining)
    - 단일노드에서 작동
    
- 필요한 기능
    - 학습된 데이터를 서버로 전송 및 서버의 hdfs에 저장하는 기능(현재는 그냥 로컬에 저장) 


## 작동시키는 방법

### server 설치
- aws EC2
- ubuntu
- [https://github.com/YBIGTA/EngineeringTeam/wiki](https://github.com/YBIGTA/EngineeringTeam/wiki)의 가이드를 따라 hadoop, spark, kafka 설치
  - topic : stream1, stream2 생성
  - 데이터 마이닝을 위한 학습 데이터를 hdfs 에 올리기
  - sparkSQL.ipynb를 이용하여 통계 데이터프레임 생성 및 저장
  - structured_streaming.ipynb을 이용해 stream처리 시작
  
- nodejs 설치
- mysql 설치
  - 설치 후 app.js에서 설정 변경
 

### clinet 작동
- pyQT, openCV, ctypes 설치
- EC2의 IP로 변경
- register 후 detection 시작
