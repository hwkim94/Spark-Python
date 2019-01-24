## 학습에 사용된 정보가 저장되는 곳
- confidence 
	- 학습 완료 후 confidence를 저장
	- confidence가 낮을수록 자기 자신으로 인식
	
- cam_size
	- (480, 360)으로 고정

- count
	- img_cnt : 촬영된 프레임의 수
	- face_cnt : 얼굴이 인식된 횟수
	- eye_cnt : 눈이 인식된 횟수

- rectangle_coord
	- 얼굴을 찾은 bounding box의 위치와 크기

- token
	-로그인 시의 token

- train_datetime
	- 학습된 시간

- train{num}.yml
	- openCV의 recognizer로 학습된 params