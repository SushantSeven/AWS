import logging
from logging.handlers import RotatingFileHandler

def init_logger():
    logger = logging.getLogger('my_logger')
    logger.setLevel(logging.INFO)

    consoleFormatter = logging.Formatter("%(asctime)s - %(name)s %(levelname)s- %(message)s", datefmt="%Y-%M-%D %H:%M:%S") # setting logging format

    consoleHandler = logging.StreamHandler() # creating stream handler
    consoleHandler.setFormatter(consoleFormatter)
    logger.addHandler(consoleHandler)

    filehandler = RotatingFileHandler('create_cluster.log') # creating file handler
    filehandler.setFormatter(consoleFormatter)
    logger.addHandler(filehandler)

    return logger

