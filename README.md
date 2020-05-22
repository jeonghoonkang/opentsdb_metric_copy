# opentsdb_metric_copy

특정 opentsdb의 metric을 쿼리해 리턴되는 데이터를 본인이 실행하는 docker container opentsdb로 복사

## 사전준비
  1.  docker/docker-compose 설치
      https://hcnam.tistory.com/25
  2. 윈도우 docker/docker-compose 설치
      https://steemit.com/kr/@mystarlight/docker


## 사용방법
  1. github repo clone
  
          git clone https://github.com/ChulseoungChae/opentsdb_metric_copy.git
          
  2. compose 디렉토리로 이동
  
          cd compose
  
  3. docker-compose.yml파일 수정(수정할 내용은 하단에 기재)

  3. docker-compose로 opentsdb container 실행

          docker-compose up -d opentsdb

  4. 1분 대기

  5. docker-compose로 opentsdb copy container 실행

          docker-compose up -d app

## docker-compose.yml파일 수정
  docker-compose.yml

        # Author : ChulseoungChae

        version: '3'

        services: 
            opentsdb:
              image: petergrace/opentsdb-docker:latest
              restart: always
              ports:
                  - "60010:4242"
              #environment:
              #    - WAITSECS=30   

            app:
              image: cschae1123/opentsdb_metric_copy:v2
              ports:
                  - "원하는 포트:22"
              volumes:
                  - "호스트 디렉토리:/app/apps/00_otsdb_copy/"
              environment:
                  - IP_ADDRESS=호스트 ip or docker-toolbox ip

   ex)

        # Author : ChulseoungChae

        version: '3'

        services: 
            opentsdb:
              image: petergrace/opentsdb-docker:latest
              restart: always
              ports:
                  - "60010:4242"
              #environment:
              #    - WAITSECS=30   

            app:
              image: cschae1123/opentsdb_metric_copy:v2
              ports:
                  - "9101:22"
              volumes:
                  - "/c/Users/tinyos/Desktop/src/compose/app_volume:/app/apps/00_otsdb_copy/"
              environment:
                  - IP_ADDRESS=192.168.99.100
                  
