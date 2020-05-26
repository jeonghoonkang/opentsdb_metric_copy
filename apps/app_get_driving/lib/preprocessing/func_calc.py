#!/usr/bin/env python2
# -*- coding: utf-8 -*-

from __future__ import print_function
import os
import sys
import time
import datetime
import json
from collections import OrderedDict
import copy
import pickle


TIME_DIFF = 3600


def run_calc(func_point):
    ret = func_point()
    return ret
    

def count_dps_aggre_count(s_ut, e_ut, _dictbuf_list, _sec):
    '''
    aggregator를 'count'으로 지정하여 초단위로 표시된 메트릭을 하루단위로 쿼리하여
    리턴된 데이터를 사용자가 지정한 특정 시간단위씩 dps 갯수를 카운트하여 반환하는 함수

    :param s_ut: 쿼리한 데이터 시작시간
    :param e_ut: 쿼리한 데이터 종료시간
    :param _dictbuf_list: 하루단위로 쿼리하여 리턴된 데이터
    :param _sec: 데이터를 묶을 시간단위
    :return: 특정 기간단위로 dps가 카운트되어 쿼리 데이터 시작시간의 value값으로 쓰여진 새로 구성된 데이터
        '''


    print('start data count...')

    for i in range(len(_dictbuf_list)):
        new_s_ut = s_ut  # 시작시간 설정
        _emtpy_dict = dict()

        if len(_dictbuf_list[i]['dps']) != 0:
            while new_s_ut != e_ut:  # 시작시간과 종료시간이 같아질 때까지 루프실행
                _count = 0
                if (s_ut + int(_sec)) == e_ut:  # 데이터 쿼리와 데이터 묶을 시간범위가 같을 때
                    _val_list = _dictbuf_list[i]['dps'].values()
                    _count = sum(_val_list)
                else:
                    # 데이터 쿼리 시간범위보다 묶을 범위가 작을 때
                    for j in range(len(_dictbuf_list[i]['dps'])):
                        # 'dps'의 타임스탬프(key)로만 정의되있는 리스트를 만들어서 카운트할 단위 시간마다 데이터를 세어 딕셔너리 재구성
                        _dict_key = _dictbuf_list[i]['dps'].keys()
                        if int(_dict_key[j]) >= new_s_ut and int(_dict_key[j]) < (new_s_ut + int(_sec)):
                            _count += _dictbuf_list[i]['dps'][j]
                new_s_ut += int(_sec)
                if _count != 0:
                    _emtpy_dict[new_s_ut - int(_sec)] = _count
            _dictbuf_list[i]['dps'] = _emtpy_dict
            # 쿼리해온 데이터의 각 'dps'에 시간단위로 카운트한 딕셔너리를 덮어쓰기함
            # print(_dictbuf_list[i]['dps'])

    # print(_dictbuf_list)

    return _dictbuf_list


def count_dps_aggre_none(s_ut, e_ut, _dictbuf_list, _sec):
    '''
    aggregator를 'none'으로 지정하여 초단위로 표시된 메트릭을 하루단위로 쿼리하여
    리턴된 데이터를 사용자가 지정한 특정 시간단위씩 dps 갯수를 카운트하여 반환하는 함수

    :param s_ut: 쿼리한 데이터 시작시간
    :param e_ut: 쿼리한 데이터 종료시간
    :param _dictbuf_list: 하루단위로 쿼리하여 리턴된 데이터
    :param _sec: 데이터를 묶을 시간단위
    :return: 특정 기간단위로 dps가 카운트되어 쿼리 데이터 시작시간의 value값으로 쓰여진 새로 구성된 데이터
        '''

    print('start data count...')

    if int(_sec) == 1:
        # 묶을 단위시간이 1초일때, 즉 1초마다 데이터 존재하는 메트릭을 생성할 때

        print('int is 1')

        # 데이터를 쿼리하면 존재하는 데이터에 대한 타임스탬프만 쿼리되기 때문에 타임스탬프 value에 1을 대입해줌
        for i in range(len(_dictbuf_list)):
            if len(_dictbuf_list[i]['dps']) != 0:
                key_list = _dictbuf_list[i]['dps'].keys()
                print(key_list)
                for j in range(len(_dictbuf_list[i]['dps'])):
                    _dictbuf_list[i]['dps'][key_list[j]] = 1
                print(_dictbuf_list[i])


    else:
        # 존재 데이터를 몇분, 몇시간, 몇일단위로 데이터를 묶을때

        for i in range(len(_dictbuf_list)):
            # print(_dictbuf_list[0])
            new_s_ut = s_ut  # 시작시간 설정
            _emtpy_dict = dict()

            if len(_dictbuf_list[i]['dps']) != 0:
                while new_s_ut != e_ut:  # 시작시간과 종료시간이 같아질 때까지 루프실행
                    _count = 0
                    if (s_ut + int(_sec)) == e_ut:  # 데이터 쿼리와 데이터 묶을 시간범위가 같을 때
                        _count = len(_dictbuf_list[i]['dps'])
                    else:
                        # 데이터 쿼리 시간범위보다 묶을 범위가 작을 때
                        for j in range(len(_dictbuf_list[i]['dps'])):
                            # 'dps'의 타임스탬프(key)로만 정의되있는 리스트를 만들어서 카운트할 단위 시간마다 데이터를 세어 딕셔너리 재구성
                            _dict_key = _dictbuf_list[i]['dps'].keys()
                            if int(_dict_key[j]) >= new_s_ut and int(_dict_key[j]) < (new_s_ut + int(_sec)):
                                _count += 1
                    new_s_ut += int(_sec)
                    if _count != 0:
                        _emtpy_dict[new_s_ut - 1] = _count
                _dictbuf_list[i]['dps'] = _emtpy_dict
                # 쿼리해온 데이터의 각 'dps'에 시간단위로 카운트한 딕셔너리를 덮어쓰기함
                # print(_dictbuf_list[i]['dps'])

    return _dictbuf_list



def empty_dps(s_ut, e_ut, _dictbuf_list, _sec):
    '''
    초단위로 표시된 메트릭을 하루단위로 쿼리하여 데이터가 없는 timestamp(empty data)들의 vlaue를
    1로 표시하여 새로운 데이터로 재구성하여 리턴하는 함수
    
    :param _dictbuf_list: 초단위로 표시된 메트릭을 하루단위로 쿼리하여 리턴된 데이터
    :return: 데이터가 없는 timestamp(empty data)들의 value를 1로 표시한 데이터
        '''
    
    new_list = []
    # if int(_sec) == 1:
    # 1초마다 데이터 존재지 않는 dps 표시된 메트릭을 생성할 때
    for data in _dictbuf_list:
        if len(data['dps']) != 0:
            _dict = dict()
            keys = data['dps'].keys()
            all_sec_list = [str(x) for x in range(int(s_ut), int(e_ut)-1)]
            diff_list = set(all_sec_list) - set(keys)
            for j in diff_list:
                _dict[str(j)] = '1'
            data['dps'] = _dict
            new_list.append(data)

    return new_list


def empty_dps_specific_time_or_more(s_ut, e_ut, _dictbuf_list, _sec):
    '''
    특정시간 이상동안 데이터가 없는 dps만 value를 1로 표시하여 데이터를 구성하여 반환하는 함수
    ex) 1시간 이상동안 빈데이터만 가지고 새로운 데이터를 만듦
    ※ empty data가 하루 이상 지속된 것은 포함하지 않는다

    :param _dictbuf_list: 하루단위 쿼리하여 리턴된 데이터
    :param _sec: 몇 시간동안 데이터가 없는것을 추려낼지 기준잡을 시간
    :return: 특정시간 이상동안 데이터가 없는 dps만 value를 1로 표시하여 구성된 데이터
        '''

    _last_keys = dict()
    #_count = 0

    if os.path.exists("./data.txt") == True:
        with open("data.txt", "rb") as f:
            _last_keys = pickle.load(f)

    for data in _dictbuf_list:
        if len(data['dps']) != 0:
            empty_time_list = list()
            carid = data['tags']['carid']  # 차번호 생성
            keys = data['dps'].keys()  # timestamp 값들을 리스트로 생성
            keys = map(int, keys)  # timestamp 값들을 정수로 변경
            keys.sort()  # timestamp 값들 정렬

            for i, key in enumerate(keys):
                if i == 0:
                    if _last_keys is None:
                        past_key = key
                        continue
                    elif str(carid) not in _last_keys.keys():
                        past_key = key
                        continue
                    else:
                        past_key = _last_keys[str(carid)]


                t_diff = key - past_key
                # 다음키에서 이전키 뺐을 때의 시간차이 구하기

                if t_diff >= 60 and t_diff < 3600:
                    empty_time_list += [str(x) for x in range(int(past_key) + 1, int(key))]

                past_key = key

            _last_keys[str(carid)] = past_key

            _dict = dict()
            for j in empty_time_list:
                _dict[str(j)] = '1'
            data['dps'] = _dict

            #_count += 1
            #print("count : %s / %s" %(_count, len(_dictbuf_list)))

    with open("data.txt", "wb") as f:
        pickle.dump(_last_keys, f)

    return _dictbuf_list


def dps_num2percent(s_ut, e_ut, _dictbuf_list, _sec):
    '''
    하루단위로 dps 갯수를 카운트한 메트릭을 쿼리하여 리턴된 데이터를 86400초 기준으로 
    백분률화 하여 데이터를 재구성하여 리턴하는 함수
    ex) 하루동안 dps 갯수가 8640라면 8640/86400 = 10%, 10을 value 값으로 한다
     
    :param _dictbuf_list: 하루단위로 dps 갯수를 카운트한 메트릭을 쿼리하여 리턴된 데이터 
    :return: 카운트된 dps 갯수를 백분률화 하여 재구성된 데이터
        '''
    
    
    print('start data conversion...')
    # print(len(_dictbuf_list))

    new_list = []
    for i in range(len(_dictbuf_list)):
        if len(_dictbuf_list[i]['dps']) != 0:
            for j in range(len(_dictbuf_list[i]['dps'])):
                _dict_key = _dictbuf_list[i]['dps'].keys()
                per_dps = float(_dictbuf_list[i]['dps'][_dict_key[j]]) / 86400 * 100
                _dictbuf_list[i]['dps'][_dict_key[j]] = round(per_dps, 2)
            new_list.append(_dictbuf_list[i])

    # print(new_list)
    # print(len(new_list))
    print('end data conversion...')
    # print(_dictbuf_list)

    return new_list


def binary_graph(s_ut, e_ut, _dictbuf_list, _sec):
    '''

    :param s_ut:
    :param e_ut:
    :param _dictbuf_list:
    :param _sec:
    :return:
        '''

    new_list = []
    print(len(_dictbuf_list))
    for data in _dictbuf_list:
        if len(data['dps']) != 0:
            new_list.append(data)

    print(len(new_list))

    return new_list



def data_count_dict(_dictbuf_list):
    print('start data count...')

    for i in range(len(_dictbuf_list)):
        _emtpy_dict = dict()
        _count = 0
        if len(_dictbuf_list[i]['dps']) != 0:
            _val_list = _dictbuf_list[i]['dps'].values()
            _count = sum(_val_list)
            _emtpy_dict['value'] = _count
            _dictbuf_list[i]['dps'] = _emtpy_dict
            # 쿼리해온 데이터의 각 'dps'에 시간단위로 카운트한 딕셔너리를 덮어쓰기함
    # print(_dictbuf_list)

    return _dictbuf_list


def reconstruction_dict(_dict_data):
    _emt_list = []

    for i in range(len(_dict_data)):
        _emt_dict = dict()
        _emt_dict['carid'] = _dict_data[i]['tags']['carid']
        _emt_dict['value'] = _dict_data[i]['dps']['value']
        _emt_dict['percentage'] = float(float(_dict_data[i]['dps']['value']) / 18489600 * 100)
        _emt_list.append(_emt_dict)

    # print(_emt_list)

    return _emt_list

def sigbrk_count(s_ut, e_ut, _dictbuf_list, _sec):
    
    new_list = []
    for data in _dictbuf_list:
        if len(data['dps']) != 0:
            empty_dict = dict()
            _count = 0
            carid = data['tags']['carid']  # 차번호 생성
            keys = data['dps'].keys()  # timestamp 값들을 리스트로 생성
        
            for k in keys:
                if int(data['dps'][k]) == 1:
                    _count += 1
            
            empty_dict[s_ut] = _count
            data['dps'] = empty_dict
            new_list.append(data)
    
    return new_list

def copy_selected_tag(s_ut, e_ut, _dictbuf_list, _sec):
    
    new_list = []
    for data in _dictbuf_list:
        if len(data['dps']) != 0:
            new_list.append(data)
    
    return new_list