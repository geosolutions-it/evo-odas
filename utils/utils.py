#!/usr/bin/python

import subprocess
import logging
import os
from logging.handlers import RotatingFileHandler

def initLogger():
    # logging setup
    if not os.path.exists('landsat_ows12.log'):
        open('landsat_ows12.log', 'w').close()
    formatter = logging.Formatter('%(name)-8s: %(levelname)-16s %(message)s')
    logging.basicConfig(level=logging.DEBUG,
                        format='%(asctime)s-6s: %(name)-10s: %(levelname)-16s %(message)s',
                        datefmt='%m-%d %H:%M:%S',
                        filename='landsat_ows12.log',
                        filemode='a')
    # log to sys.stderr (INFO level)
    console = logging.StreamHandler()
    console.setLevel(logging.INFO)
    console.setFormatter(formatter)
    logging.getLogger('').addHandler(console)
    # log to file
    logger = logging.getLogger('landsat_ows12')
    # add a rotating handler
    RotatingFileHandler('landsat_ows12.log', maxBytes=10485760, backupCount=6)
    return logger

def execute(command, output=False):
    try:
        logger = initLogger()
        logger.debug('Running command ' + str(command))
        p = subprocess.Popen(command,
                             stdout=subprocess.PIPE,
                             stderr=subprocess.STDOUT,
                             )
        if output:
            output = p.communicate()[0]
            logger.debug(output)
            return output
        else:
            output = p.communicate()
            logger.debug(output)
            return True
    except Exception as e:
        logger.error('Exception running command %s with stacktrace %s' % (command, str(e)))