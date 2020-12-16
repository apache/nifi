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

package org.apache.nifi.controller.cassandra;

public class QueryUtils {
    private QueryUtils() {}

    public static String createDeleteStatement(String keyField, String table) {
        return String.format("DELETE FROM %s WHERE %s = ?", table, keyField);
    }

    public static String createExistsQuery(String keyField, String table) {
        return String.format("SELECT COUNT(*) as exist_count FROM %s WHERE %s = ?", table, keyField);
    }

    public static String createFetchQuery(String keyField, String valueField, String table) {
        return String.format("SELECT %s FROM %s WHERE %s = ?", valueField, table, keyField);
    }

    public static String createInsertStatement(String keyField, String valueField, String table, Long ttl) {
        String retVal = String.format("INSERT INTO %s (%s, %s) VALUES(?, ?)", table, keyField, valueField);

        if (ttl != null) {
            retVal += String.format(" using ttl %d", ttl);
        }

        return retVal;
    }
}
