import logging
import logging.handlers
'''
   Setting up and return logger
'''
def getLogger(module_name, log_file):
    logger = logging.getLogger(module_name)
    logger.setLevel(logging.DEBUG)
    #logger.setLevel(logging.WARNING)

    # create file handlerRNING
    fh = logging.handlers.TimedRotatingFileHandler(log_file, "D", 1)
    fh.setLevel(logging.DEBUG)
    #fh.setLevel(logging.WARNING)

    # create console handler
    ch = logging.StreamHandler()
    ch.setLevel(logging.DEBUG)
    #ch.setLevel(logging.WARNING)

    # create formatter and add it to the handlers
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    fh.setFormatter(formatter)
    ch.setFormatter(formatter)

    # add the handlers to the logger
    logger.addHandler(fh)
    logger.addHandler(ch)

    return logger
