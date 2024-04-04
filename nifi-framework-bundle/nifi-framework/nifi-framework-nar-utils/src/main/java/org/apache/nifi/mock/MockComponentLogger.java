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
package org.apache.nifi.mock;

import org.apache.nifi.logging.ComponentLog;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Stubs out the functionality of a ComponentLog so that it can
 * be used during initialization of a component.
 */
public class MockComponentLogger implements ComponentLog {

    private static final Logger logger = LoggerFactory
            .getLogger(MockComponentLogger.class);

    @Override
    public void warn(String msg, Throwable t) {
        logger.warn(msg, t);
    }

    @Override
    public void warn(String msg, Object... os) {
        logger.warn(msg, os);
    }

    @Override
    public void warn(String msg) {
        logger.warn(msg);
    }

    @Override
    public void trace(String msg, Throwable t) {
        logger.trace(msg, t);
    }

    @Override
    public void trace(String msg, Object... os) {
        logger.trace(msg, os);
    }

    @Override
    public void trace(String msg) {
        logger.trace(msg);
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
        logger.info(msg, t);
    }

    @Override
    public void info(String msg, Object... os) {
        logger.info(msg, os);
    }

    @Override
    public void info(String msg) {
        logger.info(msg);

    }

    @Override
    public String getName() {
        return logger.getName();
    }

    @Override
    public void error(String msg, Throwable t) {
        logger.error(msg, t);
    }

    @Override
    public void error(String msg, Object... os) {
        logger.error(msg, os);
    }

    @Override
    public void error(String msg) {
        logger.error(msg);
    }

    @Override
    public void debug(String msg, Throwable t) {
        logger.debug(msg, t);
    }

    @Override
    public void debug(String msg, Object... os) {
        logger.debug(msg, os);
    }

    @Override
    public void debug(String msg) {
        logger.debug(msg);
    }
}
