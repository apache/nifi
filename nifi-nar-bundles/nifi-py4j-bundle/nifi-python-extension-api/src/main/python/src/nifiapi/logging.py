# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

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
        self.java_logger.trace(str(msg), self.__to_java_array(args))

    def debug(self, msg, *args):
        if self.min_level < LogLevel.DEBUG:
            return
        self.java_logger.debug(str(msg), self.__to_java_array(args))

    def info(self, msg, *args):
        if self.min_level < LogLevel.DEBUG:
            return
        self.java_logger.info(str(msg), self.__to_java_array(args))

    def warn(self, msg, *args):
        if self.min_level < LogLevel.DEBUG:
            return
        self.java_logger.warn(str(msg), self.__to_java_array(args))

    def error(self, msg, *args):
        if self.min_level < LogLevel.DEBUG:
            return
        self.java_logger.error(str(msg), self.__to_java_array(args))


    def __to_java_array(self, *args):
        arg_array = JvmHolder.gateway.new_array(JvmHolder.jvm.java.lang.Object, len(args))
        for i, arg in enumerate(args):
            arg_array[i] = None if arg is None else str(arg)
        return arg_array
