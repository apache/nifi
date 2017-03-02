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
 * Class for parsing double parameters and providing a user friendly error message.
 */
public class DoubleParameter {

    private static final String INVALID_DOUBLE_MESSAGE = "Unable to parse '%s' as a double value.";

    private Double doubleValue;

    public DoubleParameter(String rawDoubleValue) {
        try {
            doubleValue = Double.parseDouble(rawDoubleValue);
        } catch (NumberFormatException nfe) {
            throw new IllegalArgumentException(String.format(INVALID_DOUBLE_MESSAGE, rawDoubleValue));
        }
    }

    public Double getDouble() {
        return doubleValue;
    }
}
