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

package org.apache.nifi.controller;

import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.logging.LogLevel;

public class TerminationAwareLogger implements ComponentLog {

    private static final String TERMINATED_TASK_PREFIX = "[Terminated Process] - ";
    private final ComponentLog logger;
    private volatile boolean terminated = false;

    public TerminationAwareLogger(final ComponentLog logger) {
        this.logger = logger;
    }

    public void terminate() {
        this.terminated = true;
    }

    private boolean isTerminated() {
        return terminated;
    }

    private String getMessage(String originalMessage, LogLevel logLevel) {
        return TERMINATED_TASK_PREFIX + logLevel.name() + " - " + originalMessage;
    }


    @Override
    public void warn(String msg, Throwable t) {
        if (isTerminated()) {
            logger.debug(getMessage(msg, LogLevel.WARN), t);
            return;
        }

        logger.warn(msg, t);
    }

    @Override
    public void warn(String msg, Object... os) {
        if (isTerminated()) {
            logger.debug(getMessage(msg, LogLevel.WARN), os);
            return;
        }

        logger.warn(msg, os);
    }

    @Override
    public void warn(String msg, Object[] os, Throwable t) {
        if (isTerminated()) {
            logger.debug(getMessage(msg, LogLevel.WARN), os, t);
            return;
        }

        logger.warn(msg, os, t);
    }

    @Override
    public void warn(String msg) {
        if (isTerminated()) {
            logger.debug(getMessage(msg, LogLevel.WARN));
            return;
        }

        logger.warn(msg);
    }

    @Override
    public void trace(String msg, Throwable t) {
        if (isTerminated()) {
            logger.trace(getMessage(msg, LogLevel.TRACE), t);
            return;
        }

        logger.trace(msg, t);
    }

    @Override
    public void trace(String msg, Object... os) {
        if (isTerminated()) {
            logger.trace(getMessage(msg, LogLevel.TRACE), os);
            return;
        }

        logger.trace(msg, os);
    }

    @Override
    public void trace(String msg) {
        if (isTerminated()) {
            logger.trace(getMessage(msg, LogLevel.TRACE));
            return;
        }

        logger.trace(msg);
    }

    @Override
    public void trace(String msg, Object[] os, Throwable t) {
        if (isTerminated()) {
            logger.trace(getMessage(msg, LogLevel.TRACE), os, t);
            return;
        }

        logger.trace(msg, os, t);
    }

    @Override
    public boolean isWarnEnabled() {
        return logger.isWarnEnabled();
    }

    @Override
    public boolean isTraceEnabled() {
        return logger.isTraceEnabled();
    }

    @Override
    public boolean isInfoEnabled() {
        return logger.isInfoEnabled();
    }

    @Override
    public boolean isErrorEnabled() {
        return logger.isErrorEnabled();
    }

    @Override
    public boolean isDebugEnabled() {
        return logger.isDebugEnabled();
    }

    @Override
    public void info(String msg, Throwable t) {
        if (isTerminated()) {
            logger.debug(getMessage(msg, LogLevel.INFO), t);
            return;
        }

        logger.info(msg, t);
    }

    @Override
    public void info(String msg, Object... os) {
        if (isTerminated()) {
            logger.debug(getMessage(msg, LogLevel.INFO), os);
            return;
        }

        logger.info(msg, os);
    }

    @Override
    public void info(String msg) {
        if (isTerminated()) {
            logger.debug(getMessage(msg, LogLevel.INFO));
            return;
        }

        logger.info(msg);
    }

    @Override
    public void info(String msg, Object[] os, Throwable t) {
        if (isTerminated()) {
            logger.debug(getMessage(msg, LogLevel.INFO), os, t);
            return;
        }

        logger.info(msg, os, t);
    }

    @Override
    public String getName() {
        return logger.getName();
    }

    @Override
    public void error(String msg, Throwable t) {
        if (isTerminated()) {
            logger.debug(getMessage(msg, LogLevel.ERROR), t);
            return;
        }

        logger.error(msg, t);
    }

    @Override
    public void error(String msg, Object... os) {
        if (isTerminated()) {
            logger.debug(getMessage(msg, LogLevel.ERROR), os);
            return;
        }

        logger.error(msg, os);
    }

    @Override
    public void error(String msg) {
        if (isTerminated()) {
            logger.debug(getMessage(msg, LogLevel.ERROR));
            return;
        }

        logger.error(msg);
    }

    @Override
    public void error(String msg, Object[] os, Throwable t) {
        if (isTerminated()) {
            logger.debug(getMessage(msg, LogLevel.ERROR), os, t);
            return;
        }

        logger.error(msg, os, t);
    }

    @Override
    public void debug(String msg, Throwable t) {
        if (isTerminated()) {
            logger.debug(getMessage(msg, LogLevel.DEBUG), t);
            return;
        }

        logger.debug(msg, t);
    }

    @Override
    public void debug(String msg, Object... os) {
        if (isTerminated()) {
            logger.debug(getMessage(msg, LogLevel.DEBUG), os);
            return;
        }

        logger.debug(msg, os);
    }

    @Override
    public void debug(String msg, Object[] os, Throwable t) {
        if (isTerminated()) {
            logger.debug(getMessage(msg, LogLevel.DEBUG), os, t);
            return;
        }

        logger.debug(msg, os, t);
    }

    @Override
    public void debug(String msg) {
        if (isTerminated()) {
            logger.debug(getMessage(msg, LogLevel.DEBUG));
            return;
        }

        logger.debug(msg);
    }

    @Override
    public void log(LogLevel level, String msg, Throwable t) {
        if (isTerminated()) {
            logger.debug(getMessage(msg, level), t);
            return;
        }

        logger.log(level, msg, t);
    }

    @Override
    public void log(LogLevel level, String msg, Object... os) {
        if (isTerminated()) {
            logger.debug(getMessage(msg, level), os);
            return;
        }

        logger.log(level, msg, os);
    }

    @Override
    public void log(LogLevel level, String msg) {
        if (isTerminated()) {
            logger.debug(getMessage(msg, level));
            return;
        }

        logger.log(level, msg);
    }

    @Override
    public void log(LogLevel level, String msg, Object[] os, Throwable t) {
        if (isTerminated()) {
            logger.debug(getMessage(msg, level), os, t);
            return;
        }

        logger.log(level, msg, os, t);
    }
}
