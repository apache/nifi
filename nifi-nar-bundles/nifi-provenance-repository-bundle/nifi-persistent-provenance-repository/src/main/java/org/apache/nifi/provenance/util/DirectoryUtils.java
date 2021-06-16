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

package org.apache.nifi.provenance.util;

import java.io.File;
import java.io.FileFilter;
import java.util.Comparator;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class DirectoryUtils {
    public static final Pattern INDEX_DIRECTORY_NAME_PATTERN = Pattern.compile("(?:lucene-\\d+-)?index-(.*)");
    public static final FileFilter INDEX_FILE_FILTER = f -> INDEX_DIRECTORY_NAME_PATTERN.matcher(f.getName()).matches();
    public static final FileFilter EVENT_FILE_FILTER = f -> f.getName().endsWith(".prov") || f.getName().endsWith(".prov.gz");
    public static final Comparator<File> SMALLEST_ID_FIRST = (a, b) -> Long.compare(getMinId(a), getMinId(b));
    public static final Comparator<File> LARGEST_ID_FIRST = SMALLEST_ID_FIRST.reversed();
    public static final Comparator<File> OLDEST_INDEX_FIRST = (a, b) -> Long.compare(getIndexTimestamp(a), getIndexTimestamp(b));
    public static final Comparator<File> NEWEST_INDEX_FIRST = OLDEST_INDEX_FIRST.reversed();


    public static long getMinId(final File file) {
        final String filename = file.getName();
        final int firstDotIndex = filename.indexOf(".");
        if (firstDotIndex < 1) {
            return -1L;
        }

        final String firstEventId = filename.substring(0, firstDotIndex);
        try {
            return Long.parseLong(firstEventId);
        } catch (final NumberFormatException nfe) {
            return -1L;
        }
    }

    public static long getIndexTimestamp(final File file) {
        final String filename = file.getName();
        final Matcher matcher = INDEX_DIRECTORY_NAME_PATTERN.matcher(filename);
        if (!matcher.matches()) {
            return -1L;
        }

        try {
            return Long.parseLong(matcher.group(1));
        } catch (final NumberFormatException nfe) {
            return -1L;
        }
    }

    public static long getSize(final File file) {
        if (file.isFile()) {
            return file.length();
        }

        final File[] children = file.listFiles();
        if (children == null || children.length == 0) {
            return 0L;
        }

        long total = 0L;
        for (final File child : children) {
            total += getSize(child);
        }

        return total;
    }
}
