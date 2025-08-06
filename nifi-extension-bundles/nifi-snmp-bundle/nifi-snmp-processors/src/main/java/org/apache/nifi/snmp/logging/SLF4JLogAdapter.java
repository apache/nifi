/*_############################################################################
  _##
  _##  SNMP4J - JavaLogAdapter.java
  _##
  _##  Copyright (C) 2003-2020  Frank Fock (SNMP4J.org)
  _##
  _##  Licensed under the Apache License, Version 2.0 (the "License");
  _##  you may not use this file except in compliance with the License.
  _##  You may obtain a copy of the License at
  _##
  _##      http://www.apache.org/licenses/LICENSE-2.0
  _##
  _##  Unless required by applicable law or agreed to in writing, software
  _##  distributed under the License is distributed on an "AS IS" BASIS,
  _##  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  _##  See the License for the specific language governing permissions and
  _##  limitations under the License.
  _##
  _##########################################################################*/
package org.apache.nifi.snmp.logging;

import org.slf4j.Logger;
import org.snmp4j.log.LogAdapter;
import org.snmp4j.log.LogLevel;

import java.io.Serializable;
import java.util.Iterator;
import java.util.logging.Handler;

public class SLF4JLogAdapter implements LogAdapter {

    private final Logger logger;

    public SLF4JLogAdapter(final Logger logger) {
        this.logger = logger;
    }

    @Override
    public boolean isDebugEnabled() {
        return logger.isDebugEnabled();
    }

    @Override
    public boolean isInfoEnabled() {
        return logger.isInfoEnabled();
    }

    @Override
    public boolean isWarnEnabled() {
        return logger.isWarnEnabled();
    }

    @Override
    public void debug(final Serializable message) {
        if (isDebugEnabled()) {
            logger.debug("{}", message);
        }
    }

    @Override
    public void info(final CharSequence message) {
        if (isInfoEnabled()) {
            logger.info("{}", message);
        }
    }

    @Override
    public void warn(final Serializable message) {
        if (isWarnEnabled()) {
            logger.warn("{}", message);
        }
    }

    @Override
    public void error(final Serializable message) {
        logger.error("{}", message);
    }

    @Override
    public void error(final CharSequence message, final Throwable t) {
        logger.error("{}", message, t);
    }

    @Override
    public void fatal(final Object message) {
        logger.error("{}", message);
    }

    @Override
    public void fatal(final CharSequence message, final Throwable t) {
        logger.error("{}", message, t);
    }

    @Override
    public LogLevel getEffectiveLogLevel() {
        return LogLevel.ALL;
    }

    @Override
    public Iterator<Handler> getLogHandler() {
        throw new UnsupportedOperationException("Log handlers are not supported.");
    }

    @Override
    public LogLevel getLogLevel() {
        return getEffectiveLogLevel();
    }

    @Override
    public String getName() {
        return logger.getName();
    }

    @Override
    public void setLogLevel(final LogLevel logLevel) {
        // no need to set log level
    }
}
