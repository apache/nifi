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
package org.apache.nifi.snmp.logging;

import org.slf4j.Logger;
import org.snmp4j.log.LogAdapter;
import org.snmp4j.log.LogLevel;

import java.io.Serializable;
import java.util.Iterator;
import java.util.logging.Handler;

public class Slf4jLogAdapter implements LogAdapter {

    private final Logger logger;

    public Slf4jLogAdapter(Logger logger) {
        this.logger = logger;
    }

    // ---- Checking methods

    public boolean isDebugEnabled() {
        return true;
    }

    public boolean isInfoEnabled() {
        return true;
    }

    public boolean isWarnEnabled() {
        return true;
    }

    // ---- Logging methods

    public void debug(Serializable message) {
        log(LogLevel.DEBUG, message.toString(), null);
    }

    public void info(CharSequence message) {
        log(LogLevel.INFO, message.toString(), null);
    }

    public void warn(Serializable message) {
        log(LogLevel.WARN, message.toString(), null);
    }

    public void error(Serializable message) {
        log(LogLevel.ERROR, message.toString(), null);
    }

    public void error(CharSequence message, Throwable t) {
        log(LogLevel.ERROR, message.toString(), t);
    }

    public void fatal(Object message) {
        log(LogLevel.FATAL, message.toString(), null);
    }

    public void fatal(CharSequence message, Throwable t) {
        log(LogLevel.FATAL, message.toString(), t);
    }

    // ---- Public methods

    public LogLevel getEffectiveLogLevel() {
        return LogLevel.ALL;
    }

    public Iterator<Handler> getLogHandler() {
        return null;
    }

    public LogLevel getLogLevel() {
        return getEffectiveLogLevel();
    }

    public String getName() {
        return logger.getName();
    }

    public void setLogLevel(LogLevel logLevel) {
        // no need to set log level
    }

    // ---- Private methods

    private void log(LogLevel logLevel, String msg, Throwable t) {
        if (logLevel == LogLevel.ERROR || logLevel == LogLevel.FATAL) {
            logger.error(msg, t);
        } else if (logLevel == LogLevel.WARN) {
            logger.warn(msg);
        } else if (logLevel == LogLevel.INFO) {
            logger.info(msg);
        } else {
            logger.debug(msg);
        }
    }
}
