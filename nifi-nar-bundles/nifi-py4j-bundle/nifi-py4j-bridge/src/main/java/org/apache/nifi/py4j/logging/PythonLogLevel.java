/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.nifi.py4j.logging;

import org.apache.nifi.logging.LogLevel;

import java.util.Map;

/**
 * Python Log Level enumeration with level numbers according to Python logging module documentation
 */
public enum PythonLogLevel {
    NOTSET(0),

    DEBUG(10),

    INFO(20),

    WARNING(30),

    ERROR(40),

    CRITICAL(50);

    private static final Map<LogLevel, PythonLogLevel> FRAMEWORK_LEVELS = Map.of(
            LogLevel.NONE, NOTSET,
            LogLevel.DEBUG, DEBUG,
            LogLevel.INFO, INFO,
            LogLevel.WARN, WARNING,
            LogLevel.ERROR, ERROR,
            LogLevel.FATAL, CRITICAL
    );

    private final int level;

    PythonLogLevel(final int level) {
        this.level = level;
    }

    public int getLevel() {
        return level;
    }

    /**
     * Get Python Log Level equivalent from framework Log Level
     *
     * @param logLevel Framework Log Level
     * @return Python Log Level
     */
    public static PythonLogLevel valueOf(LogLevel logLevel) {
        return FRAMEWORK_LEVELS.getOrDefault(logLevel, WARNING);
    }
}
