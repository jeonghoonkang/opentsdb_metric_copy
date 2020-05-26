# -*- coding: utf-8 -*-

# Author : https://github.com/jeonghoonkang
# version 1.1
# 버전 주요 변경 사항 : OpenTSDB 읽기를 위한 time unit , time long 아규먼트 추가
# version 1.2
# 버전 주요 변경 사항 : carid 범위 설정, tags를 prep 내부 tags로 설정

# python library
from __future__ import print_function
import os
import sys
import time
import importlib

# 개발 코드 import
print ('==>', __file__, "importing private modules, using relative PATH" )

sys.path.append("../../lib/preprocessing")
sys.path.append('../../lib/multi_proc')
sys.path.append('../../lib/opentsdb')
sys.path.append('../../lib/log')
sys.path.append('../../lib')

'''
sys.path.append('../../apps/otsdb_dps_sum')
sys.path.append('../../apps/otsdb_empty')
sys.path.append('../../apps/otsdb_parking')
sys.path.append('../../apps/otsdb_weekly_count')
sys.path.append('../../apps/otsdb_drive_analysis')
sys.path.append('../../apps/otsdb_location')
'''

import Preprocessing
import query_drivers as qd



def brush_args():
    '''
    - 사용자 입력 인자값 읽는함수
    {read 아이피주소} {read 포트번호} {read 메트릭이름} {write 아이피주소} {write 포트} 
    {write 메트릭이름} {시작시간} {종료시간} {묶을 단위시간} {content type(dp 태그)} 
    {aggregator} {작업할 종류} {파일이름} {프로듀서 proc 갯수} {컨슈머 proc 갯수}
        '''
    usage = "\n usage : python %s {IP address} {port} " % sys.argv[0]
    usage += "{in_metric_name} {out_ip} {out_port} {out_metric} {start_day} {end_day} {partition_sec} {content type} {aggregator} {works}"
    print(usage + '\n')

    _len = len(sys.argv)

    if _len < 17:
        print(" 추가 정보를 입력해 주세요. 위 usage 설명을 참고해 주십시오")
        print(" python 이후, 아규먼트 갯수 17개 필요 ")
        print(" 현재 아규먼트 %s 개가 입력되었습니다." % (_len))
        print(" check *run.sh* file ")
        exit(" You need to input more arguments, please try again \n")

    _ip = sys.argv[1]
    _port = sys.argv[2]
    _in_met = sys.argv[3]
    _out_ip = sys.argv[4]
    _out_port = sys.argv[5]
    _out_met = sys.argv[6]

    _start = sys.argv[7]
    _end = sys.argv[8]
    _seconds = sys.argv[9] #use??
    _content = sys.argv[10]

    _aggregator = sys.argv[11]
    _works = sys.argv[12]
    _filename = sys.argv[13]
    _pn = sys.argv[14]
    _cn = sys.argv[15]
    _timeunit = sys.argv[16]
    _timelong = sys.argv[17]

    #_sort 제외
    return _ip, _port, _in_met, _out_ip, _out_port, \
        _out_met, _start, _end, _seconds, _content, \
        _aggregator, _works, _filename, _pn, _cn, \
        _timeunit, _timelong

def set_carid(start_car , end_car):
    # carid의 범위를 설정해주는 함수
    # ex) start_car = 1 , end_car = 5 | output = '1|2|3|4'

    text = ''
    for _ in range(start_car, end_car):
        text +=str(_)+'|'
    print('[start_car ~ end_car] : ' + str(start_car)+' ~ ' + str(end_car-1))

    return text[:-1]

if __name__ == "__main__":
    
    # 사용자가 입력한 인자값들을 읽어 딕셔너리 형태로 저장
    ip, port, in_metric, out_url, out_port, \
    out_metric, q_start, q_end, seconds, content, \
    aggregator, works, fname, pn, cn, \
    timeunit, timelong = brush_args()


    meta = dict()  #메타정보 저장할 딕셔너리 생성

    meta['ip'] = ip  #read 아이피주소
    meta['port'] = port  #read포트번호
    meta['in_metric'] = in_metric  #read메트릭이름
    meta['out_url'] = out_url  #write 아이피주소
    meta['out_port'] = out_port  #write 포트

    meta['out_metric'] = out_metric  #write 메트릭이름
    meta['query_start'] = q_start  #시작시간
    meta['query_end'] = q_end  #종료시간
    meta['seconds'] = seconds  #묶을 단위시간
    
    if content.upper() == 'NONE': content = '*'
    meta['content'] = content  #content type(dp태그)
    
    # 특정 tags를 입력해야 하는 경우, fname 함수 내부 변수인 tags에 할당된 값을 받아옴
    # 특정 tags 없으면 pass 
    try:
        ifunc = importlib.import_module(fname)
        try:
            if ifunc.tags:
                meta['content'] = ifunc.tags
        except:
            pass
    except :
        print (" exception : fail to import ", __file__)
        print (sys.path)
        #print(sys.modules)
        exit()
    
    meta['aggregator'] = aggregator
    meta['works'] = works
    meta['importname'] = fname    
    meta['pn'] = pn
    meta['cn'] = cn

    meta['timeunit'] = timeunit
    meta['timelong'] = timelong

    # 전체 carid 쿼리
    #meta['carid'] = "*" 
    
    # 차량 일부 쿼리
    # qd.query_nto_tsdb_v2 
    meta['carid'] = set_carid(1,606) # 1~605대 까지 

    
    print( '[ OUT METRIC NAME ]' , __file__, meta['out_metric'] )
    
    # 여기서 시간 loop 에 사용할 변수 정리
    # 처리할 시간단위, 정리
    meta['days'] = None
    meta['hrs'] = None
    meta['mins'] = None

    _tlong = meta['timelong']
    #print (_tlong)

    _tlong = int(_tlong, base=10)
    if meta['timeunit'] == 'd' :
        meta['days'] = _tlong
        if _tlong > 8 : exit("It's too long days : " + __file__ )

    elif meta['timeunit'] == 'h' :
        meta['hrs'] = _tlong
        if _tlong > 24 : exit("It's too long hours : " + __file__ )

    elif meta['timeunit'] == 'm' :
        meta['mins'] = _tlong
        if _tlong > 60 : exit("It's too long minnutes : " + __file__ )

    #print (meta)
    #exit()

    st = time.time()


    ### Link to next step
    #qd.query_nto_tsdb(meta)
    qd.query_nto_tsdb_v2(meta)
    # query_drivers.py의 query_nto_tsdb함수에 위에서 사용자가 입력한 meta정보 전달하여 실행

    et = time.time()
    print('Time elapsed : ',et-st)
    # 프로그램 동작 시간 출력
