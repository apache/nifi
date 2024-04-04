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
package org.apache.nifi.websocket.jetty.util;

import org.apache.nifi.util.StringUtils;

import java.util.AbstractMap;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public final class HeaderMapExtractor {

    private HeaderMapExtractor(){
        // Utility class, not meant to be instantiated.
    }

    public static final String HEADER_PREFIX = "header.";

    public static Map<String, List<String>> getHeaderMap(final Map<String, String> flowFileAttributes) {
        return flowFileAttributes.entrySet().stream()
                .filter(entry -> entry.getKey().startsWith(HEADER_PREFIX))
                .filter(entry -> StringUtils.isNotBlank(entry.getValue()))
                .map(entry -> new AbstractMap.SimpleImmutableEntry<>(StringUtils.substringAfter(entry.getKey(), HEADER_PREFIX), entry.getValue()))
                .collect(Collectors.toMap(Map.Entry::getKey, HeaderMapExtractor::headerValueMapper));
    }

    private static List<String> headerValueMapper(Map.Entry<String, String> entry) {
        return Arrays.stream(entry.getValue().split(",")).map(String::trim).collect(Collectors.toList());
    }

}
