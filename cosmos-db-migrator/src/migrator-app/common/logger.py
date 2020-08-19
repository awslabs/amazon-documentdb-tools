import logging


def get_logger(name):
    logging.basicConfig(
        filename='console.log',
        format='%(asctime)s %(levelname)-8s [%(filename)s:%(lineno)d]  %(message)s',
        level=logging.DEBUG, 
        datefmt='%Y-%m-%d %H:%M:%S')
    logger = logging.getLogger(name)
    logger.addHandler(__getConsoleHandler())
    return logger

def __getConsoleHandler():
    # create console handler with a higher log level
    consoleHandler = logging.StreamHandler()
    consoleHandler.setLevel(logging.INFO)
    formatter = logging.Formatter(
        fmt='%(asctime)s %(levelname)-8s [%(filename)s:%(lineno)d]  %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S')
    consoleHandler.setFormatter(formatter)
    return consoleHandler
