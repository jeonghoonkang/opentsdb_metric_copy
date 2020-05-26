# -*- coding: utf-8 -*-

from __future__ import print_function
import os
import time
import sys

import HTTP_POST_request
import Utils

import Preprocessing
import pcs
import status

import logs
import logging

import json

###################### add ######################
import operator
import pprint
###################### /add ######################


print_head = ' '*16 + '[LIB_QueryDriver]' + __file__

def calc(buf) :
    car_id_list = []
    dps_count_dict = dict()
    #print(len(_dictbuf_list))
    for data in buf:
        carid = data['tags']['VEHICLE_NUM']
        car_id_list.append(carid)
        ###################### add ######################
        dps_count_dict[carid] = len(data['dps'])
        ###################### /add ######################
    return car_id_list, dps_count_dict

def query_nto_tsdb(meta, preprocessing_method=Preprocessing.prep):
    # preprocessing_method 은 preprocessing.py 내의 prep 함수를 의미하지만,
    # 최근 사용하지 않음. 삭제 예정임

    st = time.time()

    '''
     - tsdb에서 지정 시간단위로 쿼리하고 works_basket으로 전송하는 함수
     - run.py에서 호출됨
     - meta정보와 멀티태스크마다 연결 동작할 Preprocessing.py의 prep() 함수 입력받음
     Description
     TSDB Query --> Preprocessing (multiprocessing) --> designated job ( consume )
     Params:
     - meta : { 'in_metric' : query metric,
               'out_metric' : save_metric,
               'query_start' : 날짜,
               'query_end' : 날짜,
               'dys' : 1 or None,
               'hrs' : 1 or None,
               'mins' : 1 or None,
               'seconds' : secons,
               'tag' : tag }
     - processing_method : 전처리 작업에 사용되는 함수
     - job : 전처리 이후 수행되는 작업 'consume' / 'graph' (전송/그래프 출력)
        '''

    # 프로듀서 , 컨슈머 개수 지정, pcs.py file
    if meta['pn'] == None : 
        workers = pcs.Workers(4, 4, meta)
    else :
        pn = int(meta['pn'])
        cn = int(meta['cn'])
        print ('pn cn for workers', pn, cn)
        workers = pcs.Workers(pn, cn, meta)
        
    works_basket, _ = workers.start_work(meta, preprocessing_method, \
                                                     'produce', 'consume')
    
    '''
     pcs.py의 Workers클래스의 start_work함수에 meta정보와 producer로 동작할 
     preprocessing_method (예, Preprocessing.count_dps) 함수를 전달하여 실행하고
     변수 works_basket으로 지정
        '''
    
    date_from = meta['query_start']
    date_to = meta['query_end']
    dys = meta['days']
    hrs = meta['hrs']
    mins = meta['mins']
    # 사용자에게 입력받은 메타정보들을 각 변수에 저장


    # date_from 은 string '2017/06/01-00:00:00'
    while(date_from != None):
    # date_from은 처음에는 시작 시간, 이후 요청한 시간만큼 증가
        start_time = time.time()
        q_s_date = date_from
        #print (date_from)
        queried_data, date_end = HTTP_POST_request.query_by_timedelta(date_from, meta, dys=dys, hrs=hrs, mins=mins)
      
        if date_to <= date_end : date_end = None
        # 날짜가 인자값에 설정된 end_time을 넘어가면, end_time으로 맞춰서 넘어가지 않도록 설정함
        
        if len(queried_data) != 0:
            #print( print_head, ' works_basket_put start...')
            work = (date_from, date_end, queried_data)
            # 시작시간, 종료시간, 쿼리한 데이터 듀플로 묶음

            if date_end == None:
                work = (date_from, date_to, queried_data)
            
            while (works_basket.full()):
                #pout = __file__ + 'read done, unavailable to put queue, wait 0.5 sec '
                #sys.stdout.write(pout)
                #sys.stdout.flush()
                time.sleep(0.5)
            if meta['out_metric'] != 'none' :  works_basket.put(work)
            else:
                None
            #print (" ##### Q SIZE ",works_basket.qsize())
            # 시작시간, 종료시간, 쿼리한 데이터 튜플을 works_basket에 넘겨줌
            #print (print_head,'<<<<<<Put query data to queue')

        q_e_date = date_end
        if date_end == None:
            q_e_date = date_to
        #print( print_head,'<<<Query start time>>>, Query start time : %s ,%s' % (q_s_date, q_e_date))
        run_time = time.time() - start_time
        #print( print_head," Run time for query: %.4f (sec)" % (run_time))
        #print( print_head,'>>>Query END one day<<< ')

        # 시작 날짜에서. 증가. 끝날짜 되면 None 입력
        date_from = date_end 

    dps_sum = workers.report()
    # pcs.py의 Workers 클래스의 report 함수 호출

    # dps sum logging

    et = time.time()
    logger = logging.getLogger('dps_log')
    logger = logs.log_info(logger)
    logger.setLevel(logging.INFO)

    dps_total = 0
    for dps in dps_sum:
        dps_total += dps

    print('\n'+'    Total DPS : %d' %(dps_total))
    
    try:
        logger.info('\n'+
                    '     Query Start : ' +meta['query_start'] + '\n' + 
                    '     Query End : ' + meta['query_end'] + '\n' +
                    '     Time unit : ' + meta['timeunit'] + '\n' +
                    '     Time long : ' + meta['timelong'] + '\n' +
                    '     Query url:port : ' + meta['ip']+':'+meta['port'] +'\n'+
                    '     Query Metric : ' + meta['in_metric'] + '\n'+ 
                    '     Put url:port : ' + meta['out_url']+':'+meta['out_port'] +'\n'+
                    '     Put Metric : ' + meta['out_metric'] + '\n'+ 
                    '     pn/cn : ' + meta['pn'] + '/' + meta['cn'] + '\n' +
                    '     Import file : ' + meta['importname'] + '\n'+
                    '     Content : ' + meta['content'] + '\n' +
                    '     Carid : ' + meta['carid'] + '\n' +
                    '     Total Put dps : ' + str(dps_total) + '\n' +
                    '     Total Time : ' + str(et-st))

        print('    [LOG SAVED] : /cardata/lib/log/run_info_log.log', '[PTCLL]', __file__)
    
    except:
        pass
    
    #workers.end_work()
    # pcs.py의 Workers 클래스의 end_work 함수 호출




def query_nto_tsdb_v2(meta, preprocessing_method=Preprocessing.prep):
    '''
    이전 버젼과의 차이점:
    날짜별로 Preprocessing을 나누는 것이 아니라, 
    차량별로 Preprocessing을 나눔.
    
    장점: 한번에 많은 날짜를 쿼리해도, 빠른 속도를 유지. 
    필요한 이유: non-exist 데이터는 날짜가 넘어가는 구간에 데이터가 많아서, 
                그 구간이 포함되게 쿼리해야 데이터의 유실률을 낮출 수 있다.
                긴 구간을 쿼리하면 전처리 속도가 현저히 떨어지게 된다.
                따라서 긴 구간을 쿼리 하여도 전처리 속도를 높이기 위해 필요하다  
    
    실행조건: run.py에서 carid의 범위를 설정해줘야 함 (* lib_run.py 확인)
    '''

    st = time.time()
    # carid를 설정 해줘야함. run.py 내부에서 설정. 만약 '*' 인 경우 실행이 안됨.
    # to do: 가능한 carid를 쿼리 할 수 있도록 수정..
    if meta['carid'] == '*':
        print('[CARID ERROR] If you set carid = *, use [query_nto_tsdb] ')
        print('or Check [run.py] which can control range of carid')
        exit()


    # 프로듀서 , 컨슈머 개수 지정, pcs.py file
    if meta['pn'] == None : 
        workers = pcs.Workers(4, 4, meta)
    else :
        pn = int(meta['pn'])
        cn = int(meta['cn'])
        print ('pn cn for workers', pn, cn)
        workers = pcs.Workers(pn, cn, meta)
        
    works_basket, _ = workers.start_work(meta, preprocessing_method, \
                                                     'produce', 'consume')
    
    
    date_from = meta['query_start']
    date_to = meta['query_end']
    dys = meta['days']
    hrs = meta['hrs']
    mins = meta['mins']

    if type(meta['carid']) == list:
        carid_list = meta['carid']
    else:
        carid_list = meta['carid'].split('|')
    
    # process start
    while(date_from != None):

        for carid in carid_list:
            
            meta['carid'] = carid
            start_time = time.time()
            #queried_data, date_end = HTTP_POST_request.query_by_timedelta(date_from, meta, dys=dys, hrs=hrs, mins=mins)
            queried_data, date_end = HTTP_POST_request.query_by_timedelta_v3(date_from, meta, dys=dys, hrs=hrs, mins=mins)
            
            if date_to <= date_end : date_end = None

            if len(queried_data) != 0:
                work = (date_from, date_end, queried_data)

                if date_end == None:
                    work = (date_from, date_to, queried_data)
                
                while (works_basket.full()):
                    time.sleep(0.5)
                works_basket.put(work)

            run_time = time.time() - start_time
            print( print_head," Run time for query: %.4f (sec) | [carid] : [%s]" % (run_time, carid))

        date_from = date_end 

    dps_sum = workers.report()

    # dps sum logging
    et = time.time()
    logger = logging.getLogger('dps_log')
    logger = logs.log_info(logger)
    logger.setLevel(logging.INFO)

    dps_total = 0
    for dps in dps_sum:
        dps_total += dps
        
    print('\n'+'     Total DPS : %d' %(dps_total))
    
    try:
        logger.info('\n    '+
                    ' Query Start : ' +meta['query_start'] + '\n    ' + 
                    ' Query End : ' + meta['query_end'] + '\n    ' +
                    ' Time unit : ' + meta['timeunit'] + '\n    ' +
                    ' Time long : ' + meta['timelong'] + '\n    ' +
                    ' Query url:port : ' + meta['ip']+':'+meta['port'] +'\n    '+
                    ' Query Metric : ' + meta['in_metric'] + '\n    '+ 
                    ' Put url:port : ' + meta['out_url']+':'+meta['out_port'] +'\n    '+
                    ' Put Metric : ' + meta['out_metric'] + '\n    '+ 
                    ' pn/cn : ' + meta['pn'] + '/' + meta['cn'] + '\n    ' +
                    ' Import file : ' + meta['importname'] + '\n    '+
                    ' Tags : ' + meta['content'] + '\n    ' +
                    ' Carid : ' + meta['carid'] + '\n    ' +
                    ' Total Put dps : ' + str(dps_total) + '\n    ' +
                    ' Total Time : ' + str(et-st))

        print('    [LOG SAVED] : /cardata/lib/log/run_info_log.log', '[PTCLL]', __file__)
    
    except:
        pass


def query_nto_tsdb_v3(meta, preprocessing_method=Preprocessing.prep):
    '''
    차번호마다 6/1 ~ 12/31 까지 stop과 start가 기록된 csv파일을 읽어 
    각 딕셔너리의 stop timestamp ~ start timestamp 범위로 tsdb에서 쿼리를 요청한다
    csv파일의 데이터 셋 
        - dataset : [{"stop": 1402647144, "start": 1402649959}, {"stop": 1402649975, "start": 1402653537}, ··· ]
    
    실행조건: run.py에서 carid의 범위를 설정해줘야 함 (* lib_run.py 확인)
    '''

    st = time.time()
    # carid를 설정 해줘야함. run.py 내부에서 설정. 만약 '*' 인 경우 실행이 안됨.
    # to do: 가능한 carid를 쿼리 할 수 있도록 수정..
    if meta['carid'] == '*':
        print('[CARID ERROR] If you set carid = *, use [query_nto_tsdb] ')
        print('or Check [run.py] which can control range of carid')
        exit()


    # 프로듀서 , 컨슈머 개수 지정, pcs.py file
    if meta['pn'] == None : 
        workers = pcs.Workers(4, 4, meta)
    else :
        pn = int(meta['pn'])
        cn = int(meta['cn'])
        print ('pn cn for workers', pn, cn)
        workers = pcs.Workers(pn, cn, meta)
        
    works_basket, _ = workers.start_work(meta, preprocessing_method, \
                                                     'produce', 'consume')
    

    carid_list = meta['carid'].split('|')
    # '|'와 차번호가 혼합된 문자열에서 '|'을 구분자로 해서 차번호만 뽑이내 리스트로 만든다
    
    # process start
    for carid in carid_list:

        # 차번호별로 나눠진 csv파일을 하나씩 읽는다
        with open('DATA/'+str(carid)+'_stop_start_timestamp.json', 'rb') as f:
            timestamp_list = json.load(f)
        

        for _section in range(len(timestamp_list)):
            # 각 딕셔너리의 stop timestamp ~ start timestamp 범위로 tsdb에서 쿼리를 요청한다
            meta['carid'] = carid
            start_time = time.time()

            
            date_from = Utils.unixtime2datetime(timestamp_list[_section]['stop'])
            date_end = Utils.unixtime2datetime(int(timestamp_list[_section]['start']) + int(1))
            
            queried_data = HTTP_POST_request.query_by_non_repetitive_time(date_from, date_end, meta)
            
    
            if len(queried_data) != 0:
                work = (date_from, date_end, queried_data)
                
                while (works_basket.full()):
                    time.sleep(0.5)
                works_basket.put(work)

            run_time = time.time() - start_time
            print( print_head," Run time for query: %.4f (sec) | [carid] : [%s]" % (run_time, carid))
            time.sleep(0.01)



    dps_sum = workers.report()

    # dps sum logging
    et = time.time()
    logger = logging.getLogger('dps_log')
    logger = logs.log_info(logger)
    logger.setLevel(logging.INFO)

    dps_total = 0
    for dps in dps_sum:
        dps_total += dps
        
    print('\n'+'Total DPS : %d' %(dps_total))
    
    try:
        logger.info('\n'+
                    ' Query Start : ' +meta['query_start'] + '\n' + 
                    ' Query End : ' + meta['query_end'] + '\n' +
                    ' Time unit : ' + meta['timeunit'] + '\n' +
                    ' Time long : ' + meta['timelong'] + '\n' +
                    ' Query url:port : ' + meta['ip']+':'+meta['port'] +'\n'+
                    ' Query Metric : ' + meta['in_metric'] + '\n'+ 
                    ' Put url:port : ' + meta['out_url']+':'+meta['out_port'] +'\n'+
                    ' Put Metric : ' + meta['out_metric'] + '\n'+ 
                    ' pn/cn : ' + meta['pn'] + '/' + meta['cn'] + '\n' +
                    ' Import file : ' + meta['importname'] + '\n'+
                    ' Tags : ' + meta['content'] + '\n' +
                    ' Carid : ' + meta['carid'] + '\n' +
                    ' Total Put dps : ' + str(dps_total) + '\n' +
                    ' Total Time : ' + str(et-st))

        print('[LOG SAVED] : ../lib/log/run_info_log.log')
    
    except:
        pass



def query_to_getids(meta, preprocessing_method=Preprocessing.prep):
    # preprocessing_method 은 preprocessing.py 내의 prep 함수를 의미하지만,
    # 최근 사용하지 않음. 삭제 예정임

    # DB에서 차량 ID를 검색하여 저장할 리스트
    id_list = []
    mem_old = -1
    ###################### add ######################
    dps_num_dict = dict()
    dps_percent_dict = dict()
    non_exist_dps_num_dict = dict()
    non_exist_dps_per_dict = dict()
    ###################### /add ######################
    
    st = time.time()

    '''
     - tsdb에서 지정 시간단위로 쿼리하고 works_basket으로 전송하는 함수
     - run.py에서 호출됨
     - meta정보와 멀티태스크마다 연결 동작할 Preprocessing.py의 prep() 함수 입력받음
     Description
     TSDB Query --> Preprocessing (multiprocessing) --> designated job ( consume )
     Params:
     - meta : { 'in_metric' : query metric,
               'out_metric' : save_metric,
               'query_start' : 날짜,
               'query_end' : 날짜,
               'dys' : 1 or None,
               'hrs' : 1 or None,
               'mins' : 1 or None,
               'seconds' : secons,
               'tag' : tag }
     - processing_method : 전처리 작업에 사용되는 함수
     - job : 전처리 이후 수행되는 작업 'consume' / 'graph' (전송/그래프 출력)
        '''

    # 프로듀서 , 컨슈머 개수 지정, pcs.py file
    if meta['pn'] == None : 
        workers = pcs.Workers(4, 4, meta)
    else :
        pn = int(meta['pn'])
        cn = int(meta['cn'])
        print ('pn cn for workers', pn, cn)
        workers = pcs.Workers(pn, cn, meta)
        
    works_basket, _ = workers.start_work(meta, preprocessing_method, \
                                                     'produce', 'consume')
    
    '''
     pcs.py의 Workers클래스의 start_work함수에 meta정보와 producer로 동작할 
     preprocessing_method (예, Preprocessing.count_dps) 함수를 전달하여 실행하고
     변수 works_basket으로 지정
        '''
    
    date_from = meta['query_start']
    date_to = meta['query_end']
    dys = meta['days']
    hrs = meta['hrs']
    mins = meta['mins']
    # 사용자에게 입력받은 메타정보들을 각 변수에 저장


    # date_from 은 string '2017/06/01-00:00:00'
    while(date_from != None):
    # date_from은 처음에는 시작 시간, 이후 요청한 시간만큼 증가
        start_time = time.time()
        q_s_date = date_from
        #print (date_from)
        queried_data, date_end = HTTP_POST_request.query_by_timedelta(date_from, meta, dys=dys, hrs=hrs, mins=mins)
      
        if date_to <= date_end : date_end = None
        # 날짜가 인자값에 설정된 end_time을 넘어가면, end_time으로 맞춰서 넘어가지 않도록 설정함
        
        if len(queried_data) != 0:
            #print( print_head, ' works_basket_put start...')
            work = (date_from, date_end, queried_data)
            # 시작시간, 종료시간, 쿼리한 데이터 듀플로 묶음

            if date_end == None:
                work = (date_from, date_to, queried_data)
            
            while (works_basket.full()):
                #pout = __file__ + 'read done, unavailable to put queue, wait 0.5 sec '
                #sys.stdout.write(pout)
                #sys.stdout.flush()
                time.sleep(0.5)
            if meta['out_metric'] != 'none' :  works_basket.put(work)
            else:
                ###################### add ######################
                returned_id_list, returned_dps_count = calc(queried_data)
                id_list += returned_id_list
                id_list = list(set(id_list))
                mem_count = ( len(id_list) )
                

                for k, v in returned_dps_count.iteritems():
                    if k in dps_num_dict:
                        dps_num_dict[k] = int(dps_num_dict[k]) + int(v)
                    else:
                        dps_num_dict[k] = int(v)
                ###################### /add ######################

                if (mem_old != mem_count) : 
                    pout = ' search IDs ' + str(mem_count)+'\r'
                    sys.stdout.write(pout)
                    sys.stdout.flush()

                mem_old = mem_count
                # 수신된 아이디 리스트를 추가함

        q_e_date = date_end
        if date_end == None:
            q_e_date = date_to
        #print( print_head,'<<<Query start time>>>, Query start time : %s ,%s' % (q_s_date, q_e_date))
        run_time = time.time() - start_time
        #print( print_head," Run time for query: %.4f (sec)" % (run_time))
        #print( print_head,'>>>Query END one day<<< ')

        # 시작 날짜에서. 증가. 끝날짜 되면 None 입력
        date_from = date_end
    # stdout 으로 출력된 /r 라인을 계속 보이게 하기위해, 한 줄 출력
    
    ###################### add ######################
    q_start_unixtime = HTTP_POST_request.convertTimeToEpoch(meta["query_start"])
    q_end_unixtime = HTTP_POST_request.convertTimeToEpoch(meta["query_end"])
    period_len_num = q_end_unixtime - q_start_unixtime

    for k, v in dps_num_dict.iteritems():
        dps_percent_dict[k] = round(float(float(v)*100/float(period_len_num)),1)
        non_exist_dps_value = int(period_len_num) - int(v)
        non_exist_dps_num_dict[k] = non_exist_dps_value
        non_exist_dps_per_dict[k] = round(float(float(non_exist_dps_value)*100/float(period_len_num)),1)

        

    sorted_dps_num = sorted(dps_num_dict.items(), key=operator.itemgetter(1), reverse=True)
    sorted_dps_per = sorted(dps_percent_dict.items(), key=operator.itemgetter(1), reverse=True)
    
    tot_carid_num = len(sorted_dps_num)
    grouped_dict = dict()
    carid_list = []
    _per = 10
    for i in range(len(sorted_dps_num)):
        carid_list.append(sorted_dps_num[i][0])
        if _per >= int(float(i+1) / float(tot_carid_num) * 100):
            key_name = 'within_'+str(_per)+'per'
            grouped_dict[key_name] = carid_list
            _per += 10
            carid_list = []
    #pprint.pprint(grouped_dict)
    ###################### /add ######################


    print ()

    dps_sum = workers.report()
    # pcs.py의 Workers 클래스의 report 함수 호출

    # dps sum logging

    et = time.time()
    logger = logging.getLogger('dps_log')
    logger = logs.log_info(logger)
    logger.setLevel(logging.INFO)

    dps_total = 0
    for dps in dps_sum:
        dps_total += dps

    print('\n'+'    Total DPS : %d' %(dps_total))
    
    try:
        logger.info('\n'+
                    '     Query Start : ' +meta['query_start'] + '\n' + 
                    '     Query End : ' + meta['query_end'] + '\n' +
                    '     Time unit : ' + meta['timeunit'] + '\n' +
                    '     Time long : ' + meta['timelong'] + '\n' +
                    '     Query url:port : ' + meta['ip']+':'+meta['port'] +'\n'+
                    '     Query Metric : ' + meta['in_metric'] + '\n'+ 
                    '     Put url:port : ' + meta['out_url']+':'+meta['out_port'] +'\n'+
                    '     Put Metric : ' + meta['out_metric'] + '\n'+ 
                    '     pn/cn : ' + meta['pn'] + '/' + meta['cn'] + '\n' +
                    '     Import file : ' + meta['importname'] + '\n'+
                    '     Content : ' + meta['content'] + '\n' +
                    '     Carid : ' + meta['carid'] + '\n' +
                    '     Total Put dps : ' + str(dps_total) + '\n' +
                    '     Total Time : ' + str(et-st))

        print('    [LOG SAVED] : /cardata/lib/log/run_info_log.log', '[PTCLL]', __file__)
    
    except:
        pass
    
    ###################### add ######################
    return_data = (id_list, dps_num_dict, dps_percent_dict, period_len_num, sorted_dps_num, sorted_dps_per, non_exist_dps_num_dict, non_exist_dps_per_dict, grouped_dict)
    ###################### /add ######################

    return return_data
