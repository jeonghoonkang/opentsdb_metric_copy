# -*- coding: utf-8 -*-

# Author : https://github.com/jeonghoonkang          


from __future__ import print_function
import os
import sys
import time   
import datetime
import requests
import json
from collections import OrderedDict
import copy
from multiprocessing import current_process
import importlib
import copy


# 개발 코드 import
import pcs
import pickle
import func_calc


""" 실제 작업에 필요한 query parameter 예 - 초기값 """
query_parameter = {
    "start" : "2014-06-01 00:00:00",
    "end": "2014-06-02 00:00:00",
    "aggregator" : "none",
    "metric" : "____test____"
}

""" 쿼리 하려는 tag """
query_tags = {
}

print_head = ' '*16 + '[LIB_PREPROCESSING]' + __file__



def prep(query_data, _queue , meta):
#def prep(query_data, _queue , meta, shared_val)
    try:
        ifunc = importlib.import_module(meta['importname'])
        #print ("ifunc ,   ", ifunc)
    except :
        _wcnt = 5
        while _wcnt > 0:
            _wcnt -= 1
            print (" exception : fail to import ", meta['importname'], __file__)
            #print (sys.modules)
        print (' 여기서 코드가 종료되지만, multi proc 다른 코드는 계속 실행' )
        print (' 현재 처리 부분이 예외종료 이므로, 다른 코드 동작은 의미 없음' )
        exit("EXIT")

    s_unixtime = convertTimeToEpoch(query_data[0])
    e_unixtime = convertTimeToEpoch(query_data[1])
    seconds = meta['seconds']
    start_time = time.time()

    # 위의 선언에서 import user_prep 이 지정되어 있어야 함
    # ifunc 는 app 디렉토리 내부의 실행위치 디렉토리 user_prep.py import
    # 실행 함수는 prep() 으로 고정, 테스트를 위해 ifunc.prep() 실행해서 확인
    # 테스트 - ifunc.prep()
    # ifunc.prep 내부에 오류가 있을 경우 어디에서 오류가 나는지 
    
    try:
        new_dictbuf = ifunc.prep(s_unixtime, e_unixtime, query_data[2], seconds, meta)
        
    except:        
        import traceback ; traceback.print_exc()
        print(meta['importname'] + '.prep ERROR')
        exit()
    
    #print(new_dictbuf)

    sum_dps = 0
    for dict_ in new_dictbuf:
        try:
            sum_dps+= len(dict_['dps'])
        except:        
            print(dict_)
            import traceback ; traceback.print_exc()
            print(meta['importname'] + '.prep ERROR')
            exit()

    print('sum_dps', sum_dps)

    len_dictbuf = len(new_dictbuf)
    if len_dictbuf != 0: # check list size
        _putIteration(new_dictbuf, len_dictbuf, _queue, meta)
    # tsdb에 put할 형식으로 변환후 queue에 전송한다, 이후 자동으로 전송됨 by 컨수머 프로세스
    
    run_time = time.time() - start_time
    #print( print_head, " Run time for query, dict 분할, 준비 완료 (%s %s): %.4f (sec)" % (query_data[0], query_data[1], run_time))

    return 'ALL DONE', sum_dps


def _putIteration(_dict_list, _len, _queue, meta):
    # TSDB에 put할 형식으로 데이터를 전처리하고 queue에 전송함

    putDict = dict()
    dict_array = list()
    #_append = dict_array.append
    _len_list = len(_dict_list)
    #print( print_head, _putIteration.__name__, 'dict list length', _len_list)
    putDict['metric'] = str(meta['out_metric'])
    _current_cnt = 0

    for _ix in xrange(_len_list) :
        #print ( ' '*4,  '..... alive' , _ix)
        #print (_dict_list[_ix])

        for k, v in _dict_list[_ix].iteritems():
            #print ( ' '*4, ' '*4, '..... alive' , k)
            # want to handle only 'dps' key with dps existing
            if k != 'dps' : continue

            
            _len_dp = len(v)
            #print ( ' '*4, ' '*4, ' '*4,'..... alive' , _len_dp)
            if _len_dp == 0 : continue
            
            putDict['tags'] = _dict_list[_ix]['tags']

            xcount = 0
            for kt, pv in v.iteritems() :

                putDict['timestamp'] = kt
                putDict['value'] = pv
                dict_array.append(copy.deepcopy(putDict))
                
                # 50개씩 put
                if len(dict_array) == 50:
                    #xcount += 1
                    #print ( ' '*4, ' '*4, ' '*4, ' '*4,'..... alive' , kt, xcount)
                    if _queue.full() == True :
                        print ( '[CRITICAL]', __file__, ' producer preperation Queue is full ')
                        exit ( '[CRITICAL]' + __file__ + 'check QueuSzie')
                    _queue.put(dict_array)
                    if _queue.qsize() > 550000 :
                        print (__file__, "Buffer Full" )
                        time.sleep(0.3)

                    #print (print_head, 'dict array', dict_array)
                    dict_array = list() #initialize

                    
        if len(dict_array) != 0: # if dict_array is empty( [] ), Don't put
            # 50개씩 put하고 나머지 put
            _queue.put(dict_array)
            _last_tm = dict_array[-1]['timestamp']
            
        dict_array = list() # initialize

        _current_cnt += 1

        if _current_cnt % 50 == 0 : 
            __display_out =  '\n' + " "*70 + print_head + " %s / %s \r" % (_current_cnt, _len_list)
            #sys.stdout.write(__display_out)
            #sys.stdout.flush()

    if _current_cnt == _len_list:
        None
        #print (" "*70 + print_head + " %s / %s " % (_current_cnt, _len_list))
        #print( print_head, '%s ===== End put ==========' %ts2datetime(_last_tm))

    return True


def strday_delta(_s, _h_scale):
    # 문자열 날짜, 시간정보를 지정시간만큼 더해줌
    
    _dt  = str2datetime(_s)
    if _h_scale == 24 : _dt += datetime.timedelta(days = 1)
    elif _h_scale == 1 : _dt += datetime.timedelta(hours = 1)
    elif _h_scale == 0.1 : _dt += datetime.timedelta(minutes = 20)
    _str = datetime2str(_dt)
    return _str


def daydelta(_date_on, _date_off, dys=None, hrs=None, mins=None):
    # 시작시간과 종료시간이 같을때 None을 반환해줌

    _on = str2datetime(_date_on)
    _off = str2datetime(_date_off)
    if dys != None:
        _delta = _on + datetime.timedelta(days=1)
    elif hrs != None:
        _delta = _on + datetime.timedelta(hours=1)
    elif mins != None:
        _delta = _on + datetime.timedelta(minutes=20)
    if _delta == _off:
        return None
    else:
        return datetime2str(_delta)


def str2datetime(dt_str):
    # 문자열 날짜,시간정보를 datetime.datetime 클래스 객체로 반환
    return datetime.datetime.strptime(dt_str, "%Y/%m/%d-%H:%M:%S")


def datetime2str(dt):
    # 날짜,시간 정보를 문자열로 바꿔줌
    return dt.strftime('%Y/%m/%d-%H:%M:%S')


def convertTimeToEpoch(_time):
    # 날짜,시간 정보를 유닉스 타임으로 바꿔줌
    _f_time_sym_ = ''

    if _time.find('/') != -1:
        _f_time_sym_ = '/'
    if _time.find('-') != -1:
        _f_time_sym_ = '-'

    date_time = "%s.%s.%s %s:%s:%s" \
                %(_time[8:10], _time[5:7], _time[:4], _time[-8:-6], _time[-5:-3], _time[-2:])
    pattern = '%d.%m.%Y %H:%M:%S'
    epoch = int (time.mktime(time.strptime(date_time, pattern)))

    return epoch


def ts2datetime(ts_str):
    return datetime.datetime.fromtimestamp(int(ts_str)).strftime('%Y/%m/%d-%H:%M:%S')
