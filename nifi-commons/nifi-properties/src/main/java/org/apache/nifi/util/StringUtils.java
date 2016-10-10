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

    public static final String EMPTY = "";

    public static boolean isBlank(final String str) {
        if (str == null || str.isEmpty()) {
            return true;
        }
        for (int i = 0; i < str.length(); i++) {
            if (!Character.isWhitespace(str.charAt(i))) {
                return false;
            }
        }
        return true;
    }

    public static boolean isEmpty(final String str) {
        return str == null || str.isEmpty();
    }

    public static boolean startsWith(final String str, final String prefix) {
        if (str == null || prefix == null) {
            return (str == null && prefix == null);
        }
        if (prefix.length() > str.length()) {
            return false;
        }
        return str.regionMatches(false, 0, prefix, 0, prefix.length());
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

    public static String join(final Collection collection, String delimiter) {
        if (collection == null || collection.size() == 0) {
            return EMPTY;
        }
        final StringBuilder sb = new StringBuilder(collection.size() * 16);
        for (Object element : collection) {
            sb.append((String) element);
            sb.append(delimiter);
        }
        return sb.toString().substring(0, sb.lastIndexOf(delimiter));
    }

    public static String padLeft(final String source, int length, char padding) {
        if (source != null) {
            StringBuilder sb = new StringBuilder(source).reverse();
            while (sb.length() < length) {
                sb.append(padding);
            }
            return sb.reverse().toString();
        }
        return null;
    }

    public static String padRight(final String source, int length, char padding) {
        if (source != null) {
            StringBuilder sb = new StringBuilder(source);
            while (sb.length() < length) {
                sb.append(padding);
            }
            return sb.toString();
        }
        return null;
    }
}
