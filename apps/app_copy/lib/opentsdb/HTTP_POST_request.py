#!/usr/bin/env python2
# -*- coding: utf-8 -*-
"""
    Created on Mon Mar  5 14:41:11 2018

    @author: Taewoo
             https://github.com/jeonghoonkang
    """


from __future__ import print_function

import time
import requests
import json

from collections import OrderedDict
from multiprocessing import current_process

import Utils

print_head = ' '*16 + '[LIB_OPENTSDB]'

MAX_BUFFER = 1000000
#requests.adapters.DEFAULT_RETRIES = 8

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


def convertTimeToEpoch(_time):
    date_time = "%s.%s.%s %s:%s:%s" %(_time[8:10], _time[5:7], _time[:4], _time[-8:-6], _time[-5:-3], _time[-2:])
    #print date_time
    pattern = '%d.%m.%Y %H:%M:%S'
    epoch = int (time.mktime(time.strptime(date_time, pattern)))
    return epoch


def _processingResponse(in_data):
    '''openTSDB에서 전송받은, string 들을 dictionary로 변경하여
       dict 를 회신함. 형태를 변경하고, dict의 갯수를 알려줌'''
    _d = in_data
    _r = _d.json()
    # queryData.content is string, thus convert this to list
    _l = len(_r)

    return _r, _l


def query_by_timedelta(_date, meta, dys=None, hrs=None, mins=None):
    global query_parameter
    
    if dys != None : 
        _t_scale = meta['days']
        _type = 'days'
    elif hrs != None : 
        _t_scale = meta['hrs']
        _type = 'hrs'
    elif mins != None : 
        _t_scale = meta['mins']
        _type = 'mins'

    assert type(_t_scale)==int, 'not integer for t_scale'
    query_parameter['start'] = _date
    # delta 시간만큼 시간 변환한 string
    query_parameter['end'] = Utils.strday_delta(_date, _type, _t_scale)
    query_parameter['aggregator'] = meta['aggregator']
    query_parameter['metric'] = meta['in_metric']

    query_tags['VEHICLE_NUM'] = meta['carid']
    query_tags['fieldname'] = meta['content']

    _q_para = query_parameter
    _url = 'http://' + meta['ip'] + ':' + meta['port'] + '/api/query'

    queryData = QueryData(_url, _q_para, query_tags)
    _dictbuf, _dictlen = _processingResponse(queryData)
    
    return _dictbuf, query_parameter['end']

# 여러 메트릭 
def query_by_timedelta_v3(_date, meta, dys=None, hrs=None, mins=None):
    global query_parameter
    
    if dys != None : 
        _t_scale = meta['days']
        _type = 'days'
    elif hrs != None : 
        _t_scale = meta['hrs']
        _type = 'hrs'
    elif mins != None : 
        _t_scale = meta['mins']
        _type = 'mins'

    assert type(_t_scale)==int, 'not integer for t_scale'
    query_parameter['start'] = _date
    # delta 시간만큼 시간 변환한 string
    query_parameter['end'] = Utils.strday_delta(_date, _type, _t_scale)
    query_parameter['aggregator'] = meta['aggregator']
    
    metric_list = meta['in_metric'].split('|')
    return_dictbuf=[]
    for _metric in metric_list:
        query_parameter['metric'] = _metric

        query_tags['VEHICLE_NUM'] = meta['carid']
        
        _q_para = query_parameter
        _url = 'http://' + meta['ip'] + ':' + meta['port'] + '/api/query'

        queryData = QueryData(_url, _q_para, query_tags)
        
        _dictbuf, _dictlen = _processingResponse(queryData)
        for _dict in _dictbuf:
            return_dictbuf.append(_dict)
    
    return return_dictbuf, query_parameter['end']

def query_by_non_repetitive_time(q_start_time, q_end_time, meta):
    global query_parameter

    #print(print_head, __file__, 'query starting...')


    query_parameter['start'] = q_start_time
    query_parameter['end'] = q_end_time
    query_parameter['aggregator'] = meta['aggregator']
    query_parameter['metric'] = meta['in_metric']

    query_tags['VEHICLE_NUM'] = meta['carid']

    _q_para = query_parameter
    _url = 'http://' + meta['ip'] + ':' + meta['port'] + '/api/query'

    queryData = QueryData(_url, _q_para, query_tags)
    _dictbuf, _dictlen = _processingResponse(queryData)

    return _dictbuf


def QueryData(_url, _required, _tags=None):
    headers = {'content-type': 'application/json'}

    dp = OrderedDict()    # dp (Data Point)
    dp["start"] = convertTimeToEpoch(_required["start"])
    dp["end"] = convertTimeToEpoch(_required["end"]) - int(1)   # not exactly required

    temp = OrderedDict()
    temp["aggregator"] = _required["aggregator"]
    temp["metric"] = _required["metric"]
    if _tags != None:
        temp["tags"] = _tags

    dp["queries"] = []
    dp["queries"].append(temp)
    #print (print_head, json.dumps(dp))

    #print " [Querying]" + json.dumps(dp, ensure_ascii=False, indent=4)
    response = requests.post(_url, data=json.dumps(dp), headers= headers)

    while response.status_code > 204:
        print(print_head," [Bad Request] Query status: %s" % (response.status_code))
        print(print_head," [Bad Request] We got bad request, Query will be restarted after 3 sec!\n")
        time.sleep(3)

        print(print_head," [Querying]" + json.dumps(dp, ensure_ascii=False, indent=4))
        response = requests.post(_url, data=json.dumps(dp), headers= headers)

    pout = " [Query is done, got reponse from server]" + __file__
    pout += " : now starting processing, writing and more "
    #print(print_head,pout)
    return response




''' 실질적으로 OpenTSDB에 HTTP POST 방식으로 PUT Request를 보내는 함수
    50개로 multiple data 를 put 하는게 가장 좋으며, 늘어날 때는 테스트 필요함
    http://opentsdb.net/docs/build/html/api_http/put.html '''
def putRequest(_session, _url, _buffer):
    ''' put sends json packs to opentsdb, since opentsdb runs on a multi-thread mode
        putRequest runs efficiently parallelized '''
    headers = {'content-type': 'application/json'}

    for i in xrange(0, len(_buffer), 50):
        #print json.dumps(_buffer[i:i+50], ensure_ascii=False, indent=4)
        response = _session.post(_url, data=json.dumps(_buffer[i:i+50]), headers= headers)
        while response.status_code > 204:
            print (print_head, "error!")
            print (print_head, response)
            response = _session.post(_url, data=json.dumps(_buffer[i:i+50]), headers= headers)
            

        if i+1 % 10000 == 0:
            print ("\tputData: %s / %s finished" % (i+1, len(_buffer)))

def guaranteePutRetry(_session, _url, _buf, _time):
    ''' Prevent sending too many requests from same ip address in short period of time '''
    while(True):
        try:
            putRequest(_session, _url, _buf)
        except requests.exceptions.ConnectionError:
            print ("<%s> -> RETRY after %ssec" % (current_process().name, str(_time)))
            time.sleep(_time)
            continue
        break


''' 데이터를 받아와서 JSON 형태로 만들어 buf에 담고 putRequest 함수로 전달 '''
def PutData(_session, _url, _metric, _content, _carid, _data):
    buf = []
    keys = _data.keys()

    for k in keys:
        dp = OrderedDict()    # dp (Data Point)

        dp["metric"] = _metric
        dp["timestamp"] = int(k)
        dp["value"] = int(_data[k])
        #print dp["value"]

        dp["tags"] = OrderedDict()
        dp["tags"]["content"] = _content
        dp["tags"]["carid"] = _carid

        buf.append(dp)

        if len(buf) >= MAX_BUFFER:
            guaranteePutRetry(_session, _url, buf, 3)
            buf = []

    guaranteePutRetry(_session, _url, buf ,3)
