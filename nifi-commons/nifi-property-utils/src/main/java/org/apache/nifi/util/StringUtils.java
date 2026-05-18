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

import java.util.Collection;

/**
 * String Utils based on the Apache Commons Lang String Utils.
 * These simple util methods here allow us to avoid a dependency in the core
 */
public class StringUtils {

    public static final String SPACE = " ";

    public static final String EMPTY = "";

    public static boolean isBlank(final String str) {
        return str == null || str.isBlank();
    }

    public static boolean isNotBlank(final String str) {
        return !isBlank(str);
    }

    public static boolean isEmpty(final String str) {
        return str == null || str.isEmpty();
    }

    public static boolean isNotEmpty(final String str) {
        return !isEmpty(str);
    }

    public static boolean startsWith(final String str, final String prefix) {
        if (str == null || prefix == null) {
            return (str == null && prefix == null);
        }
        if (prefix.length() > str.length()) {
            return false;
        }
        return str.startsWith(prefix);
    }

    public static String substringAfter(final String str, final String separator) {
        if (isEmpty(str)) {
            return str;
        }
        if (separator == null) {
            return EMPTY;
        }
        int pos = str.indexOf(separator);
        if (pos == -1) {
            return EMPTY;
        }
        return str.substring(pos + separator.length());
    }

    public static String join(final Collection<?> collection, String delimiter) {
        if (collection == null || collection.isEmpty()) {
            return EMPTY;
        }

        if (collection.size() == 1) {
            return String.valueOf(collection.iterator().next());
        }

        final StringBuilder sb = new StringBuilder();
        for (final Object element : collection) {
            sb.append(element);
            sb.append(delimiter);
        }

        return sb.substring(0, sb.lastIndexOf(delimiter));
    }
}
