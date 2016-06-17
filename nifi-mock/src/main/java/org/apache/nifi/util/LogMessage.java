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

import org.slf4j.Marker;

public class LogMessage {
    private final Marker marker;
    private final String msg;
    private final Throwable throwable;
    private final Object[] args;
    public LogMessage(final Marker marker, final String msg, final Throwable t, final Object... args) {
        this.marker = marker;
        this.msg = msg;
        this.throwable = t;
        this.args = args;
    }

    public Marker getMarker() {
        return marker;
    }
    public String getMsg() {
        return msg;
    }
    public Throwable getThrowable() {
        return throwable;
    }
    public Object[] getArgs() {
        return args;
    }

    @Override
    public String toString() {
        return this.msg;
    }
}