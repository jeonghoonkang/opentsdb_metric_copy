#!/bin/bash

# Author : https://github.com/jeonghoonkang
# 파이션 코드 실행스크립트
# version 1.1
# 버전 주요 변경 사항 : OpenTSDB 읽기를 위한 time unit , time long 아규먼트 추가
# meta 삭제


echo " 시작날짜, 끝날짜는 최소 하루단위로 지정해야함. 최소 24시간 이상 (날짜 뒤에 시간은 -00:00:00으로 해줘야한다) "

# read 할 open tsdb 정보
url_in='125.140.110.217'
port_in=4242
#metric_in='HanuriTN_00'
metric_in='_kang_HNTN_spd_0001_1130_1207_'

# write 할 open tsdb 정보
url_out='localhost'
port_out=4242
metric_out='_kang_count_HNT_1130_1207_'

time_start='2014/11/30-00:00:00'
time_end='2014/12/07-00:00:00'

seconds='86400'

tag='None'
agg='none'
id='num'

# import 할 파일 이름, ./user_prep.py
file='user_prep'

# 생성할 프로세서 갯수. Producer, Consumer
pn='4'
cn='8'

# read 단위를 결정, d 하루단위 tsdb read, h 시간단위, m 분단위
timeunit='m'
timelong='15'


echo ">>===================================================="
echo "실행 관련 주요 정보"
echo $metric_in $metric_out $url_out $time_start $time_end
echo $url_in $port_in $metric_in $url_out $port_out $metric_out $time_start $time_end $seconds $tag $agg $id $file $pn $cn $timeunit $timelong
echo "====================================================<<"

# time 은 스크립트 SW 실행 시간을 확인하기 위해 사용 
# 인자값 17개 
time python lib_run.py $url_in $port_in $metric_in $url_out $port_out $metric_out $time_start $time_end $seconds $tag $agg $id $file $pn $cn $timeunit $timelong

echo " *** end script run for PYTHON *** "
