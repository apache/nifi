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
package org.apache.nifi.util;

import org.apache.nifi.logging.ComponentLog;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

public class MockComponentLog implements ComponentLog {

    private final CapturingLogger logger;
    private final Object component;

    public MockComponentLog(final String componentId, final Object component) {
        this.logger = new CapturingLogger(LoggerFactory.getLogger(component.getClass()));
        this.component = component;
    }

    /**
     * @return a {@link List} of all TRACE level messages recorded by this {@link Logger}.
     */
    public List<LogMessage> getTraceMessages() {
        return logger.getTraceMessages();
    }

    /**
     * @return a {@link List} of all DEBUG level messages recorded by this {@link Logger}.
     */
    public List<LogMessage> getDebugMessages() {
        return logger.getDebugMessages();
    }

    /**
     * @return a {@link List} of all INFO level messages recorded by this {@link Logger}.
     */
    public List<LogMessage> getInfoMessages() {
        return logger.getInfoMessages();
    }

    /**
     * @return a {@link List} of all WARN level messages recorded by this {@link Logger}.
     */
    public List<LogMessage> getWarnMessages() {
        return logger.getWarnMessages();
    }

    /**
     * @return a {@link List} of all ERROR level messages recorded by this {@link Logger}.
     */
    public List<LogMessage> getErrorMessages() {
        return logger.getErrorMessages();
    }

    private Object[] addProcessor(final Object[] originalArgs) {
        return prependToArgs(originalArgs, component);
    }

    private Object[] addProcessorAndFormattedThrowable(final Object[] originalArgs) {
        if (originalArgs != null && originalArgs.length > 0 && (originalArgs[originalArgs.length - 1] instanceof Throwable)) {
            final Object[] modifiedArgs = new Object[originalArgs.length + 2];

            modifiedArgs[0] = component.toString();
            System.arraycopy(originalArgs, 0, modifiedArgs, 1, originalArgs.length - 1);
            modifiedArgs[modifiedArgs.length - 2] = originalArgs[originalArgs.length - 1].toString();
            modifiedArgs[modifiedArgs.length - 1] = originalArgs[originalArgs.length - 1];

            return modifiedArgs;
        } else {
            return addProcessor(originalArgs);
        }
    }

    private Object[] prependToArgs(final Object[] originalArgs, final Object... toAdd) {
        final Object[] newArgs = new Object[originalArgs.length + toAdd.length];
        System.arraycopy(toAdd, 0, newArgs, 0, toAdd.length);
        System.arraycopy(originalArgs, 0, newArgs, toAdd.length, originalArgs.length);
        return newArgs;
    }

    private boolean lastArgIsException(final Object[] os) {
        return (os != null && os.length > 0 && (os[os.length - 1] instanceof Throwable));
    }

    @Override
    public void warn(final String msg, final Throwable t) {
        warn("{} " + msg, component, t);
    }

    @Override
    public void warn(String msg, Object... os) {
        if (lastArgIsException(os)) {
            os = addProcessorAndFormattedThrowable(os);
            msg = "{} " + msg + ": {}";

            logger.warn(msg, os);
        } else {
            os = addProcessor(os);
            msg = "{} " + msg;
            logger.warn(msg, os);
        }
    }

    @Override
    public void warn(String msg) {
        msg = "{} " + msg;
        logger.warn(msg, component);
    }

    @Override
    public void trace(String msg, Throwable t) {
        msg = "{} " + msg;
        final Object[] os = {component};
        logger.trace(msg, os, t);
    }

    @Override
    public void trace(String msg, Object... os) {
        if (lastArgIsException(os)) {
            os = addProcessorAndFormattedThrowable(os);
            msg = "{} " + msg + ": {}";

            logger.trace(msg, os);
        } else {
            os = addProcessor(os);
            msg = "{} " + msg;
            logger.trace(msg, os);
        }
    }

    @Override
    public void trace(String msg) {
        msg = "{} " + msg;
        final Object[] os = {component};
        logger.trace(msg, os);
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
        msg = "{} " + msg;
        final Object[] os = {component};

        logger.info(msg, os);
        if (logger.isDebugEnabled()) {
            logger.info("", t);
        }
    }

    @Override
    public void info(String msg, Object... os) {
        if (lastArgIsException(os)) {
            os = addProcessorAndFormattedThrowable(os);
            msg = "{} " + msg + ": {}";

            logger.info(msg, os);
        } else {
            os = addProcessor(os);
            msg = "{} " + msg;
            logger.info(msg, os);
        }
    }

    @Override
    public void info(String msg) {
        msg = "{} " + msg;
        final Object[] os = {component};

        logger.info(msg, os);
    }

    @Override
    public String getName() {
        return logger.getName();
    }

    @Override
    public void error(String msg, Throwable t) {
        msg = "{} " + msg;
        final Object[] os = {component};

        logger.error(msg, os, t);
        if (logger.isDebugEnabled()) {
            logger.error("", t);
        }
    }

    @Override
    public void error(String msg, Object... os) {
        if (lastArgIsException(os)) {
            os = addProcessorAndFormattedThrowable(os);
            msg = "{} " + msg + ": {}";

            logger.error(msg, os);
        } else {
            os = addProcessor(os);
            msg = "{} " + msg;
            logger.error(msg, os);
        }
    }

    @Override
    public void error(String msg) {
        msg = "{} " + msg;
        final Object[] os = {component};

        logger.error(msg, os);
    }

    @Override
    public void debug(String msg, Throwable t) {
        msg = "{} " + msg;
        final Object[] os = {component};

        logger.debug(msg, os, t);
    }

    @Override
    public void debug(String msg, Object... os) {
        if (lastArgIsException(os)) {
            os = addProcessorAndFormattedThrowable(os);
            msg = "{} " + msg + ": {}";

            logger.debug(msg, os);
        } else {
            os = addProcessor(os);
            msg = "{} " + msg;
            logger.debug(msg, os);
        }
    }

    @Override
    public void debug(String msg) {
        msg = "{} " + msg;
        final Object[] os = {component};

        logger.debug(msg, os);
    }
}
