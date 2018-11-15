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
package org.apache.nifi.documentation.init;

import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.logging.LogLevel;

public class NopComponentLog implements ComponentLog {
    @Override
    public void warn(final String msg, final Throwable t) {

    }

    @Override
    public void warn(final String msg, final Object[] os) {

    }

    @Override
    public void warn(final String msg, final Object[] os, final Throwable t) {

    }

    @Override
    public void warn(final String msg) {

    }

    @Override
    public void trace(final String msg, final Throwable t) {

    }

    @Override
    public void trace(final String msg, final Object[] os) {

    }

    @Override
    public void trace(final String msg) {

    }

    @Override
    public void trace(final String msg, final Object[] os, final Throwable t) {

    }

    @Override
    public boolean isWarnEnabled() {
        return false;
    }

    @Override
    public boolean isTraceEnabled() {
        return false;
    }

    @Override
    public boolean isInfoEnabled() {
        return false;
    }

    @Override
    public boolean isErrorEnabled() {
        return false;
    }

    @Override
    public boolean isDebugEnabled() {
        return false;
    }

    @Override
    public void info(final String msg, final Throwable t) {

    }

    @Override
    public void info(final String msg, final Object[] os) {

    }

    @Override
    public void info(final String msg) {

    }

    @Override
    public void info(final String msg, final Object[] os, final Throwable t) {

    }

    @Override
    public String getName() {
        return null;
    }

    @Override
    public void error(final String msg, final Throwable t) {

    }

    @Override
    public void error(final String msg, final Object[] os) {

    }

    @Override
    public void error(final String msg) {

    }

    @Override
    public void error(final String msg, final Object[] os, final Throwable t) {

    }

    @Override
    public void debug(final String msg, final Throwable t) {

    }

    @Override
    public void debug(final String msg, final Object[] os) {

    }

    @Override
    public void debug(final String msg, final Object[] os, final Throwable t) {

    }

    @Override
    public void debug(final String msg) {

    }

    @Override
    public void log(final LogLevel level, final String msg, final Throwable t) {

    }

    @Override
    public void log(final LogLevel level, final String msg, final Object[] os) {

    }

    @Override
    public void log(final LogLevel level, final String msg) {

    }

    @Override
    public void log(final LogLevel level, final String msg, final Object[] os, final Throwable t) {

    }
}
