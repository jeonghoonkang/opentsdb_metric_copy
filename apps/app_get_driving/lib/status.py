# -*- coding: utf-8 -*-

from __future__ import print_function
import sys
import os
import logging
import multiprocessing
try:
    import Queue
except ModuleNotFoundError:
    import queue
import time

def check(status_basket, total):
    current = 0
    print('Total no. of files : ', total)
    printProgressBar(current, total)
    while True:
        done = status_basket.get()
        
        if done == None:
            break
        current += 1
        printProgressBar(current, total)
    
def printProgressBar(iteration, total, prefix = 'Progress', suffix = 'Complete',\
                      decimals = 1, length = 100, fill = '█'): 
    """ 
    Call in a loop to create terminal progress bar
    @params:
        iteration   - Required  : current iteration (Int)
        total       - Required  : total iterations (Int)
        prefix      - Optional  : prefix string (Str)
        suffix      - Optional  : suffix string (Str)
        decimals    - Optional  : positive number of decimals in percent complete (Int)
        length      - Optional  : character length of bar (Int)
        fill        - Optional  : bar fill character (Str)
        """
    percent = ("{0:." + str(decimals) + "f}").format(100 * (iteration / float(total)))
    filledLength = int(length * iteration // total)
    bar = fill * filledLength + '-' * (length - filledLength)
    print('\r%s |%s| %s%% %s' % (prefix, bar, percent, suffix), end='\r')
    sys.stdout.flush()
    if iteration == total: 
        print()

class Status:
    def __init__(self, total, status_basket):
        self.total = total
        self.current = 0
        self.status_basket = status_basket

    def start_checking(self):
        self.status_checker = multiprocessing.Process(target=check,\
                                                      args=(self.status_basket, self.total))
        self.status_checker.start()
    
    def end_checking(self):
        self.status_basket.put(None)
        self.status_checker.join()

        
        

'''
class Failed_points_Logger(logging.LoggerAdapter):
    def __init__(self):
        pid = str(os.getpid())
        _logger = logging.getLogger('CONSUMER.LOGS')
        fhandler = logging.handlers.RotatingFileHandler('consumer.logs.'+pid+'.log',\
                                       maxBytes=67108864, delay=True)
        _logger.addHandler(fhandler)
        super(Failed_points_Logger,self).__init(_logger,extra)

    def process(self, msg, kwargs):
        return '%s' % (kwargs['extra']) 

    '''









