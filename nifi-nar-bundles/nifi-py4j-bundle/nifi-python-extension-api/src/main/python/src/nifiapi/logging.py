from nifiapi.__jvm__ import JvmHolder
from enum import Enum

class LogLevel(Enum):
    TRACE = 1,
    DEBUG = 2,
    INFO = 3,
    WARN = 4,
    ERROR = 5


class Logger:
    def __init__(self, min_level, java_logger):
        self.min_level = min_level
        self.java_logger = java_logger

    def trace(self, msg, *args):
        if self.min_level < LogLevel.DEBUG:
            return
        self.java_logger.trace(msg, self.__to_java_array__(args))

    def debug(self, msg, *args):
        if self.min_level < LogLevel.DEBUG:
            return
        self.java_logger.debug(msg, self.__to_java_array__(args))

    def info(self, msg, *args):
        if self.min_level < LogLevel.DEBUG:
            return
        self.java_logger.info(msg, self.__to_java_array__(args))

    def warn(self, msg, *args):
        if self.min_level < LogLevel.DEBUG:
            return
        self.java_logger.warn(msg, self.__to_java_array__(args))

    def error(self, msg, *args):
        if self.min_level < LogLevel.DEBUG:
            return
        self.java_logger.error(msg, self.__to_java_array__(args))


    def __to_java_array__(self, *args):
        arg_array = JvmHolder.gateway.new_array(JvmHolder.jvm.java.lang.Object, len(args))
        for i, arg in enumerate(args):
            arg_array[i] = arg
        return arg_array
