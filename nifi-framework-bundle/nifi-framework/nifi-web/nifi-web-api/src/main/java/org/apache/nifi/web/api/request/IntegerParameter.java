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
package org.apache.nifi.web.api.request;

/**
 * Class for parsing integer parameters and providing a user friendly error message.
 */
public class IntegerParameter {

    private static final String INVALID_INTEGER_MESSAGE = "Unable to parse '%s' as an integer value.";

    private Integer integerValue;

    public IntegerParameter(String rawIntegerValue) {
        try {
            integerValue = Integer.parseInt(rawIntegerValue);
        } catch (NumberFormatException nfe) {
            throw new IllegalArgumentException(String.format(INVALID_INTEGER_MESSAGE, rawIntegerValue));
        }
    }

    public Integer getInteger() {
        return integerValue;
    }
}
