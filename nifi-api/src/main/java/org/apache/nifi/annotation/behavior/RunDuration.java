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
package org.apache.nifi.annotation.behavior;

public enum RunDuration {
    ZERO_MILLIS(0),
    TWENTY_FIVE_MILLIS(25),
    FIFTY_MILLIS(50),
    ONE_HUNDRED_MILLIS(100),
    TWO_HUNDRED_FIFTY_MILLIS(250),
    FIVE_HUNDRED_MILLIS(500),
    ONE_SECONDS(1000),
    TWO_SECONDS(2000);

    private final long millis;

    RunDuration(final long millis) {
        this.millis = millis;
    }

    public long getMillis() {
        return millis;
    }
}
