# -*- coding: utf-8 -*-
import logging



# test.log 파일에 저장
def log_set(pid, logger):
    # 하나의 로거 사용
    # pid : process id

    # 로그 레벨, 파일명, 함수명, 라인명, 메세지
    formatter = logging.Formatter('[%(levelname)s] [%(asctime)s] [%(filename)s-%(funcName)s:%(lineno)d] %(message)s' + '\n')

    fileH = logging.FileHandler('/app/lib/log/run_log_'+pid+'.log') # 로그파일 명에 로그 발생한 프로세스 명을 내포
    streamH = logging.StreamHandler() # 콘솔창에 로그가 보이게

    fileH.setFormatter(formatter)
    streamH.setFormatter(formatter)

    logger.addHandler(fileH)
    logger.addHandler(streamH)

    return logger


def log_info(logger):

    # 로그 레벨, 파일명, 함수명, 라인명, 메세지
    formatter = logging.Formatter('[%(levelname)s] [%(asctime)s] %(message)s' + '\n')
    fileH = logging.FileHandler('../../lib/log/run_info_log.log')
    fileH.setFormatter(formatter)
    logger.addHandler(fileH)

    return logger
