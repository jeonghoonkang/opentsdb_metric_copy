# -*- coding: utf-8 -*-

# Author : https://github.com/CHOBO1
#          https://github.com/jeonghoonkang          

from __future__ import print_function
import os
import requests
import time
import json
import multiprocessing
import sys
# from pathos.helpers import mp
# from pathos.multiprocessing import ProcessingPool as Pool


# 개발코드 import
import keti_multiprocessing

''' 
    #main 테스트중일때 아래 경로 추가되어 있어야 함
import sys
sys.path.append('../opentsdb')
sys.path.append('../')
    '''     

import HTTP_POST_request

# log
import logs


# 코드 기능 업그레이드 시, 멀티 프로세시에서는 arg 를 arg*로 받기 때문에 갯수에 주의해야 함
def _produce(works_to_do, works_done, status_basket, meta, preprocessor):
#def _produce(works_to_do, works_done, status_basket, meta, preprocessor, shx):
    '''
    Producer function for sending preprocessed data to consumer processes
    (Never called directly)
    Params:
    - works_to_do : queue object for getting a path(string) of csv file
    - works_done : queue object for passing preprocessed data to consumer processes
        '''

    # 만일 이전에 실행된 apply 코드가 에러가 있으면 아래 라인은 출력되지 않음
    #print (__file__, works_to_do)
    #print (__file__, works_done)
    #print (__file__, status_basket)
    #print (__file__, meta)
    #print (__file__, preprocessor)
    #print ("########### in the process >>>_produce called correctly   ###########")

    #print (__file__, shx)
    #print ('address = ', shx)
    #print ('value = ', shx.value)
    #exit()

    dps_total_sum = 0
    while True:
        #print ('loop producer')
        while (works_to_do.empty()):
            _pout = ' 프로듀서 큐 Producer queue is empty, wait for 1 \n'
            _pout = __file__ + _pout 
            #sys.stdout.write(_pout)
            #sys.stdout.flush()
            time.sleep (1)

        _pout = ' 프로듀서 큐 Producer queue has data packet  ___________________\n'
        _pout = __file__ + _pout 
        #sys.stdout.write(_pout)
        #sys.stdout.flush()

        #print (help('modules'))

        fpath = works_to_do.get()  # fpath : csv file path or OpenTSDB return JSON
        '''
        except queue.Empty as e :
             print ("Queue empty...............")
             continue 
            '''
        
        if fpath == None:
            break

        #logger = multiprocessing.log_to_stderr()
        #logger.setLevel(multiprocessing.SUBDEBUG)
        #logger.setLevel(logging.INFO)
        
        # preprocess raw the data(fpath file or OpenTSDB return JSON) 
        # and send them to consumers
        # shv.value 는 멀티프로세스간 공유된 변수의 값을 argument에 입력
        # http 로 TSDB 전송할때 발생한 IO Error 발생 횟수임
        #ret = preprocessor(fpath, works_done, meta, shx.value)
        #
        ret, dps_sum = preprocessor(fpath, works_done, meta)

        dps_total_sum += dps_sum

        if status_basket != None:
            status_basket.put(fpath)


    return dps_total_sum

print_head = ' '*4 + '[LIB_MULTIPOC pcs.py]' + 'Sending 2 TSDB'

def _consume(works_to_do, url, meta):
#def _consume(works_to_do, url, meta, shx):
    '''
    Consumer function for sending preprocessed data to tsdb
    (Never called directly)

    Params:
    - works_to_do 큐 (in_queue) : queue object for getting a preprocessed data from producer processes
        '''
    '''
    # 만일 이전에 실행된 apply 코드가 에러가 있으면 아래 라인은 출력되지 않음
    # print ("########### in the process >>>_consumer called correctly   ###########")
    # print (__file__, works_to_do)
    # print (__file__, meta)
    # print (__file__,shx)
    # print ('address = ', shx)
    # print ('value = ', shx.value)
        '''
    pid = str(os.getpid())
    failed_dps_buffer = []
    total_log_files_cnt = 0
    log_cnt = 0

    try:
        sess = requests.Session()
        headers = {'content-type': 'application/json'}
        #headers = {'content-type': 'application/json', 'connection': 'keep-alive'}
        logf = None

    except requests.ConnectTimeout as e:
        print('ERROR : cannot connect to the server')
        exit()

    _wcnt = 0
    # get an item from queue(in_queue, works_to_do) and post it to tsdb
    while True:
        _wcnt += 1
        
        if works_to_do.empty() == True:
            #_pout = '    Consumer Queue is Empty ! %d \n ' %_wcnt
            #_pout = __file__ + _pout 
            #sys.stdout.write(_pout)
            #sys.stdout.flush()
            time.sleep(2)
            continue
        

        item = works_to_do.get()
        
        #_pout = "...get buffer... %d \r" %_wcnt
        #sys.stdout.write (_pout)
        #sys.stdout.flush()

        _qsz = works_to_do.qsize()
        if (_qsz % 250) == 0 :
            _pout = print_head + '  Consumer Queue Size %d     ||| \r' %(_qsz)
            sys.stdout.write(_pout)
            sys.stdout.flush()

        # 로그 고의 생성
        #item = []

        if item != None:
            try:
                #print ("I------", type(item), len(item))
                # item의 디셔너리 갯수를 확인해보니 1일 query 데이터가 14,440개 정도임
                # 기본적으로 50개로 구성된 배열 1개가 적합함 (참조: openTSDB 웹페이지)
                response = sess.post(url, data=json.dumps(item), headers=headers)  # , timeout=10)
                #response = HTTP_POST_request.putRequest(sess, url, data=json.dumps(item))
    
                tries = 0
                # 서버로부터 응답이 오긴 하는데 ACK가 아닌 경우
                while (response.status_code > 204):
                    # try to change  50 dps -> 30 dps
                    print ( 'http put error', response, __file__)
                    print("[Process ID] " + pid)
                    
                    # 로그 저장
                    #logger.error('\n[Bad Request] response status: %s' % (response.status_code)+
                    #             '\n'+ '[DataPoint] ' + json.dumps(item, ensure_ascii=False, indent=4))

                    if tries > 10:
                        print('Tried count exceed')
                        # json.dump(item, flogger)
                        failed_dps_buffer.append(item)
                        log_cnt += 1
                        if log_cnt > 100:
                            total_log_files_cnt += 1
                            log_cnt = 0
                            # open file and write to file
                            # logf = open(log_dir +'/'+total_log_files_cnt+'.dps', mode='w')
                            # logf = open('./' + str(total_log_files_cnt) + '.dps', mode='w')
                            #json.dump(failed_dps_buffer, logf)
                            failed_dps_buffer = []
                            #logf.close()

                        tries = 0
                        break

                    time.sleep(0.05 * tries)
                    response = sess.post(url, data=json.dumps(item), headers=headers)  # , timeout=10)
                    #response = HTTP_POST_request.putRequest(sess, url, data=json.dumps(item))
                    tries += 1
                    
            # 서버로부터 아예 응답이 없는 경우
            except requests.ReadTimeout as e:
                retry = 0
                while True:
                    if retry > 10:
                        print('ERROR : NO RESPONSE FROM SERVER. EXITING THIS PROGRAM')
                        total_log_files_cnt += 1
                        failed_dps_buffer.append(item)

                        # logf = open(log_dir +'/'+total_log_files_cnt+'.dps', mode='w')
                        logf = open('./' + str(total_log_files_cnt) + '.dps', mode='w')
                        json.dump(failed_dps_buffer, logf)
                        logf.close()
                        exit()
                    try:
                        sess = requests.Session()
                        break
                    except requests.ConnectTimeout as e:
                        retry += 1
                        time.sleep(10)
            
            # 기타 IO 관련 에러
            # 체크할수 있는 플래그가 있어야 하지만, 아직 구현 못함
            # 현재는 Queue max 갯수를 full 로 체크하다가, full 되면 종료시켜 버림
            except IOError as e:
                #shx.value += 1
                print( '#'*3, __file__, 'ERROR : IOError. system exits')
                # to do : 이 경우, 지속적으로 Query 하는 것을 막아줘야 함 

                total_log_files_cnt += 1
                failed_dps_buffer.append(item)

                # logf = open(log_dir +'/'+total_log_files_cnt+'.dps', mode='w')
                logf = open('./' + str(total_log_files_cnt) + '.dps', mode='w')
                json.dump(failed_dps_buffer, logf)
                logf.close()
                exit()
        else:
            break

    if len(failed_dps_buffer) != 0:
        total_log_files_cnt += 1
        # logf = open(log_dir +'/'+total_log_files_cnt+'.dps', mode='w')
        logf = open('./' + str(total_log_files_cnt) + '.dps', mode='w')
        json.dump(failed_dps_buffer, logf)

    if logf != None:
        logf.close()
    return True


def _graph_and_save(works_to_do, save_location, meta):
    '''
    main_run작업을 수행하는 프로세스로부터 얻은 전처리된 데이터(카운트)를 바탕으로
    그래프를 생성하여 저장하는 작업 수행
        '''
    pass


JOBS = {'produce': _produce,
        'consume': _consume,
        'graph': _graph_and_save}


class Workers:
    '''
    전처리작업 프로세스와 이 결과를 전송하는 프로세스 관리 클래스

    일반적은 호출 순서는 다음과 같다:
    1. 객체 생성 (__init__)
    2. start_work() : 작업내용 및 정보 제공 (내부적으로 프로세스 생성)
    3. end_work() : 작업 마침표 전송
    4. report() : 프로세스 풀을 닫음

        '''

    def __init__(self, nP, nC, \
                 meta, logs=False):
        '''
        Params:
        - nP : 프로듀서(전처리 작업) 프로세스 개수
        - nC : 컨슈머(전송) 프로세스 갯수 개수
            '''
               
        url='http://'+meta['out_url']+':'+meta['out_port']+'/api/put'
        print(nP, nC, url, logs)
        self.nP = nP
        self.nC = nC

        if nP == 0:
            raise AttributeError('nP should be at least 1')

        # 데이터 송수신 큐 설정
        self.pwork_basket = multiprocessing.Manager().Queue(maxsize=2)
        self.cwork_basket = None
        self.status_basket = None
        if nC != 0:
            self.cwork_basket = multiprocessing.Manager().Queue(maxsize=600000)
            # maxsize는 preproccessor의 iteration에 영향을 줌
            # (1일 기준) 86400초 * 600대, 50초(개) 묶음, 150 Byte 정도로 계산함

        if logs:
            self.status_basket = multiprocessing.Manager().Queue(maxsize=300)

        self.works_done_location = url

        #프로세스간 공유를 위한 변수 i 정수 / d 더블프리시전 
        #공유변수 i 는 process 생성할때, 같이 해야함
        self.shared_x = multiprocessing.Value('i', 0)


    def start_work(self, meta, preprocessor, job1='produce', job2='consume'):
        '''
         프로듀서에게 할 일(preprocessor)과 관련 메타데이터를 전달하여 전처리를 수행하도록 지시

         Params:
         - meta : 전처리 작업에 필요한 메타데이터
                 {'metric' : 메트릭명,
                  'target_dir' : 전처리파일이 있는 디렉토리경로,
                  'dir' : 전처리 결과가 저장될 디렉토리경로(전처리 결과를 바로 db에 전송할 경우x
                 }
         - preprocessor : Preprocessing 모듈에 정의된 전처리 함수
         - job1 : producer process가 실행할 작업 (전처리)
         - job2 : consumer process가 실행할 작업 (tsdb로 전송 / 그래프로 저장)
            '''

        # print (self.shared_x, self.shared_x.value)
        # 프로듀서, 컨슈머 생성
        self.producers = keti_multiprocessing.mproc(self.nP, 'task1-worker')
        if self.nC != 0:
            self.consumers = keti_multiprocessing.mproc(self.nC, 'task2-worker')
 
        '''
        # 멀티프로세싱의 문제는 코드가 에러가 있을경우, 아무런 출력도 화면에 나오지 않아서
        # 마치 잘 동작하는것으로 착각을 일으킬 때가 있음
        # 아래 2줄로 코드가 제대로 동작하는지 확인하면 좋음
        # 확인 또 확인은 귀찮지만 전체 소요 시간을 줄여줄것이라 믿음
        # print (globals())
        # exit()
        
        # 특히, 새로운 process 생성할 때 에러가 많은데,
        # 미리 여기서 테스트해보고 apply 에 들어가는 패러미터를 결정하는 것이 좋아 보임
        # print ('address = ', self.shared_x)
        # print ('value = ', self.shared_x.value)
        
        # 프로세스 시작 위에서 nP, nC 로 생성한 오브젝트에 실제 동작할 함수를 map 해주는 과정임
        # apply() => user code
            '''
        if self.nP != 0:
            # self.producers 는 mproc 클래스 
            #self.producers.apply(JOBS[job1], (self.pwork_basket, self.cwork_basket, \
            #                     self.status_basket, meta, preprocessor, self.shared_x))
            self.producers.apply(JOBS[job1], self.pwork_basket, self.cwork_basket, \
                                 self.status_basket, meta, preprocessor)
        
        '''
        # 만일 위의 apply 코드가 에러가 있으면 아래 라인은 출력되지 않음
        # print ("########### process created & apply succeed ###########")
        # 아래 exit()가 샐행되면, spawn 된 process는 살아 있고, 현재 프로세스는 종료되어야 함
        # shell SW인 htop 으로 확인해 볼것
            '''
        """
         producers.apply()
         producers는 class Workers의 __init__함수에서 keti_multiprocessing.mproc에서 함수 실행을 병렬화 선언한다
         pwork_basket, cwork_basket, status_basket는 class Workers의 __init__함수에서 queue로 설정해줬다
         apply 함수 : apply(function, (args))는 함수 이름과 그 함수의 인수를 입력으로 받아 간접적으로 함수를 실행시키는 명령어
            ex) apply( sum, (3,4,) ) <- 인수 3과 4를 입력으로 받아 sum함수를 실행
                출력 : 7
         프로듀서 프로세스 갯수가 0이 아닐때 
         인수들[pwork_basket, cwork_basket, status_basket, meta, preprocessor(전처리함수)]을 입력으로 받아서
         JOBS[job1] = _produce 함수(이 코드의 line17)에 전달하여 실행시킨다
         따라서 producers를 병렬화 선언후 _produce에 각 queue와 메타정보, preprocessor함수를 인자로 주어 실행시키는 것이다 
            """

        if self.nC != 0:
        #   self.consumers.apply(JOBS[job2], (self.cwork_basket, self.works_done_location, meta, self.shared_x))
           self.consumers.apply(JOBS[job2], self.cwork_basket, self.works_done_location, meta)

        '''
         consumers.apply()
         consumers는 class Workers의 __init__함수에서 keti_multiprocessing.mproc에서 함수 실행을 병렬화 선언한다

         cwork_basket은 class Workers의 __init__함수에서 queue로 설정해줬다

         apply 함수 : apply(function, (args))는 함수 이름과 그 함수의 인수를 입력으로 받아 간접적으로 함수를 실행시키는 명령어
            ex) apply(sum, (3,4)) <- 인수 3과 4를 입력으로 받아 sum함수를 실행
                출력 : 7

         프로듀서 프로세스 갯수가 0이 아닐때 
         인수[cwork_basket, works_done_location(Workers 클래스의 __init__함수에서 정의된 url='http://125.140.110.217:54242/api/put?'),meta]
         을 입력으로 받아서 JOBS[job2] = _consume 함수(이 코드의 line40)에 전달하여 실행시킨다
         따라서 consumers를 병렬화 선언후 _consume에 cwork_basket queue와 url, 메타정보를 인자로 주어 실행시키는 것이다 
            '''

        print('Workers-start success')

        return (self.pwork_basket, self.status_basket)

    """
    def end_work(self):
        '''
        프로듀서에게 더이상의 할 일은 없음을 알린다
            '''
        #sentinel 전송
        for _ in range(self.nP):
            self.pwork_basket.put(None)
        for _ in range(self.nC):
            self.cwork_basket.put(None)
            """

    def report(self):
        '''
        프로듀서와 컨슈머 프로세스로부터 결과 리턴
        내부적으로 멀티프로세스 pool을 close, join함
            '''
        for _ in range(self.nP):
            self.pwork_basket.put(None)

        if self.nP != 0:
            # except 없이 수행이 잘 되면 제대로 return
            dps_sum = self.producers.get()

        for _ in range(self.nC):
            self.cwork_basket.put(None)

        if self.nC != 0:
            ret2 = self.consumers.get()

        return dps_sum
        


if __name__ == '__main__':
    g = globals()
    print (g)

    print ('...start...')
    meta = dict()
    meta['out_port']='4242'
    w = Workers(1,1, meta)
    preprocessor = None
    stopf = 0
    w.start_work(meta, preprocessor, stopf)
    print ('...finish...')
