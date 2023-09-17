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
package org.apache.nifi.rules.handlers;

import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.logging.LogLevel;
import org.apache.nifi.logging.LogMessage;

public class MockComponentLog implements ComponentLog {

    String infoMessage;
    String warnMessage;
    String debugMessage;
    String traceMessage;
    String errorMessage;

    protected String convertMessage(String msg, Object[] os){
        String replaceMsg = msg;
        for (Object o : os) {
            replaceMsg = replaceMsg.replaceFirst("\\{\\}", os.toString());
        }
        return replaceMsg;
    }

    @Override
    public void warn(String msg, Throwable t) {
        warn(msg);
    }

    @Override
    public void warn(String msg, Object... os) {
        warn(msg);
    }

    @Override
    public void warn(String msg, Object[] os, Throwable t) {
        warn(convertMessage(msg,os));
    }

    @Override
    public void warn(String msg) {
        warnMessage = msg;
    }

    @Override
    public void warn(LogMessage logMessage) {
        warnMessage = logMessage.getMessage();
    }

    @Override
    public void trace(String msg, Throwable t) {
        trace(msg);
    }

    @Override
    public void trace(String msg, Object... os) {
        trace(msg);
    }

    @Override
    public void trace(String msg) {
        traceMessage = msg;
    }

    @Override
    public void trace(String msg, Object[] os, Throwable t) {
        trace(convertMessage(msg,os));
    }

    @Override
    public void trace(LogMessage logMessage) {
        traceMessage = logMessage.getMessage();
    }

    @Override
    public boolean isWarnEnabled() {
        return true;
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
        return true;
    }

    @Override
    public void info(String msg, Throwable t) {
        info(msg);
    }

    @Override
    public void info(String msg, Object... os) {
        info(msg);
    }

    @Override
    public void info(String msg) {
        infoMessage = msg;
    }

    @Override
    public void info(String msg, Object[] os, Throwable t) {
        info(convertMessage(msg,os));
    }

    @Override
    public void info(LogMessage message) {
        infoMessage = message.getMessage();
    }

    @Override
    public String getName() {
        return null;
    }

    @Override
    public void error(String msg, Throwable t) {
        error(msg);
    }

    @Override
    public void error(String msg, Object... os) {
        error(convertMessage(msg, os));
    }

    @Override
    public void error(String msg) {
        errorMessage = msg;
    }

    @Override
    public void error(String msg, Object[] os, Throwable t) {
        error(msg);
    }

    @Override
    public void error(LogMessage message) {
        errorMessage = message.getMessage();
    }

    @Override
    public void debug(String msg, Throwable t) {
        debug(msg);
    }

    @Override
    public void debug(String msg, Object... os) {
        debug(convertMessage(msg, os));
    }

    @Override
    public void debug(String msg, Object[] os, Throwable t) {
        debug(msg);
    }

    @Override
    public void debug(String msg) {
        debugMessage = msg;
    }

    @Override
    public void debug(LogMessage message) {
        debugMessage = message.getMessage();
    }

    @Override
    public void log(LogLevel level, String msg, Throwable t) {

    }

    @Override
    public void log(LogLevel level, String msg, Object... os) {

    }

    @Override
    public void log(LogLevel level, String msg, Object[] os, Throwable t) {

    }

    public String getInfoMessage() {
        return infoMessage;
    }

    public String getWarnMessage() {
        return warnMessage;
    }

    public String getDebugMessage() {
        return debugMessage;
    }

    public String getTraceMessage() {
        return traceMessage;
    }

    public String getErrorMessage() {
        return errorMessage;
    }


}
