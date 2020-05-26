#!/bin/bash

# Author : https://github.com/jeonghoonkang
# 파이션 코드 실행스크립트
# version 1.1
# 버전 주요 변경 사항 : 아규먼트 18개로 추가


echo " 시작날짜, 끝날짜는 최소 하루단위로 지정해야함. 최소 24시간 이상 (날짜 뒤에 시간은 -00:00:00으로 해줘야한다) "

# read 할 open tsdb 정보
# [1] 
url_in='125.140.110.217'
# [2]
port_in=54242
# [3]
metric_in='Elex.2020.01.remove_outlier_v2'

#target_id=50

# write 할 open tsdb 정보
# [4]
#url_out='125.140.110.217'
#url_out='192.168.99.100'
url_out=$IP_ADD
# [5]
#port_out=4242
#port_out=54242
port_out=$PORT
# [6]
metric_out='Elex.2020.01.test'
# [7]
time_start='2020/01/01-00:00:00'
# [8]
time_end='2020/01/15-00:00:00'
# [9]
seconds='86400'

  # processingtype IDEACH, IDALL, IDSELECT, IDFILE
  # IDEACH, 각 자동차 아이디 마다 처리
  #         --> id 변수에 처리할 자동차 아이디 지정
  # IDALL, 모든 자동차 아이디 처리
  # IDSELECT, 몇개의 자동차 아이디를 선별적으로 처리
  # IDFILE, 파일을 읽어서, 해당 아이디를 처리함. 파일 내부에는 배열로 아이디가 저장
  #         --> idfile.py , 내부에는 ids=[] 리스트 존재
  # none : 처리 방법을 명시하지 않음, 일부 기능에서는 ID 종류 선정이 필요 없음

# [10]
processingtype='none'

# [11] 주의할점은 '*'로 입력하면 쉘 명령으로 동작함. 사용하면 안됨 
content='DRIVE_SPEED'
# [12]
agg='none'
# [13] # id is carid
id='saved_id'

# [14]
file='user_prep'
# import 할 파일 이름, ./user_prep.py

# 생성할 프로세서 갯수. Producer, Consumer
# [15]
pn='2'
# [16]
cn='2'

# read 단위를 결정, d 하루단위 tsdb read, h 시간단위, m 분단위
# [17]
timeunit='d'
# [18]
timelong='1'


echo ">>===================================================="
echo "실행 관련 주요 정보"
echo $metric_in $metric_out $url_out $time_start $time_end
#    [1]     [2]      [3]        [4]      [5]       [6]         [7]         [8]       [9]      [10]            [11]    [12] [13] [14] [15] [16] [17]     [18]
echo $url_in $port_in $metric_in $url_out $port_out $metric_out $time_start $time_end $seconds $processingtype $content $agg $id $file $pn $cn $timeunit $timelong
#[16] [17]    [18]
echo "====================================================<<"


# time 은 스크립트 SW 실행 시간을 확인하기 위해 사용 
# 인자값 18개 
#                   [1]    [2]      [3]        [4]      [5]       [6]         [7]         [8]       [9]      [10]            [11]    [12] [13] [14] [15] [16] [17]     [18]
python run.py $url_in $port_in $metric_in $url_out $port_out $metric_out $time_start $time_end $seconds $processingtype $content $agg $id $file $pn $cn $timeunit $timelong
#[13] [14] [15] [16] [17]   [18] 

echo " *** end script run for PYTHON *** "
