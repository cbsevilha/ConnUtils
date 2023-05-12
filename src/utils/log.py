import logging
import sys


class Logger:
    FORMAT = "%(asctime)s<%(name)s.%(funcName)-s>[%(levelname)-5s]-%(message)s"

    @staticmethod
    def get_console_handler():
        console_handler = logging.StreamHandler(sys.stdout)
        console_handler.setFormatter(logging.Formatter(Logger.FORMAT))
        return console_handler

    @staticmethod
    def get_logger(logger_name: str):
        logger = logging.getLogger(logger_name)

        if not logger.handlers:
            # better to have too much log than not enough
            logger.setLevel(logging.DEBUG)

            logger.addHandler(Logger.get_console_handler())

            # with this pattern, it's rarely necessary
            # to propagate the error up to parent
            logger.propagate = False

        return logger


class WithLogger(type):
    def __new__(cls, name, bases, attr):
        ret_inst = super(WithLogger, cls).__new__(cls, name, bases, attr)
        ret_inst.logger = Logger().get_logger(name)
        return ret_inst

    def __call__(self, *args, **kwargs):
        return super(WithLogger, self).__call__(*args, **kwargs)


# base class for inheritance
class Logging(metaclass=WithLogger):
    pass
