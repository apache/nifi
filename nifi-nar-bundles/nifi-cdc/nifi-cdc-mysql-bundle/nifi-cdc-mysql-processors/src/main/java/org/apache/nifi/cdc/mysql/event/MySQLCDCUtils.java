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
package org.apache.nifi.cdc.mysql.event;

import java.io.Serializable;

/**
 * A utility class to provide MySQL- / binlog-specific constants and methods for processing events and data
 */
public class MySQLCDCUtils {

    public static Object getWritableObject(Integer type, Serializable value) {
        if (value == null) {
            return null;
        }
        if (type == null) {
            if (value instanceof byte[]) {
                return new String((byte[]) value);
            } else if (value instanceof Number) {
                return value;
            }
        } else if (value instanceof Number) {
            return value;
        } else {
            if (value instanceof byte[]) {
                return new String((byte[]) value);
            } else {
                return value.toString();
            }
        }
        return null;
    }
}
