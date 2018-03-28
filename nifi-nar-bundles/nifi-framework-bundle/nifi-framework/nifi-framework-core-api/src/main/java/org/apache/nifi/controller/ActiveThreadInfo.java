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

public class ActiveThreadInfo {
    private final String threadName;
    private final String stackTrace;
    private final long activeMillis;
    private final boolean terminated;

    public ActiveThreadInfo(final String threadName, final String stackTrace, final long activeMillis, final boolean terminated) {
        this.threadName = threadName;
        this.stackTrace = stackTrace;
        this.activeMillis = activeMillis;
        this.terminated = terminated;
    }

    public String getThreadName() {
        return threadName;
    }

    public String getStackTrace() {
        return stackTrace;
    }

    public long getActiveMillis() {
        return activeMillis;
    }

    public boolean isTerminated() {
        return terminated;
    }
}
