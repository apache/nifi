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

package org.apache.nifi.syslog.utils;

/**
 * Policy for handling Structured Data
 * must match the simple-syslog-5424 StructuredDataPolicy
 */
public enum NifiStructuredDataPolicy {
    /**
     * The Structured Data will be flattened per the KeyProvider provided values.
     */
    FLATTEN,
    /**
     * The Structued Data will be returned as a Map field named structuredData.
     * Each map entry will have the value of the Structured Data ID, and a value
     * of a map of each element param name and value
     */
    MAP_OF_MAPS
}
