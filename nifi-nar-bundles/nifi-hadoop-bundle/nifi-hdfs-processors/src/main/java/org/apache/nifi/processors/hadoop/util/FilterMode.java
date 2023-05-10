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
package org.apache.nifi.processors.hadoop.util;

import org.apache.nifi.components.DescribedValue;

import java.util.stream.Stream;

import static org.apache.nifi.processors.hadoop.ListHDFS.FILE_FILTER;
import static org.apache.nifi.processors.hadoop.ListHDFS.RECURSE_SUBDIRS;

public enum FilterMode implements DescribedValue {

    FILTER_DIRECTORIES_AND_FILES(
            "filter-mode-directories-and-files",
            "Directories and Files",
            "Filtering will be applied to the names of directories and files.  If " + RECURSE_SUBDIRS.getDisplayName()
                    + " is set to true, only subdirectories with a matching name will be searched for files that match "
                    + "the regular expression defined in " + FILE_FILTER.getDisplayName() + "."
    ),
    FILTER_MODE_FILES_ONLY(
            "filter-mode-files-only",
            "Files Only",
            "Filtering will only be applied to the names of files.  If " + RECURSE_SUBDIRS.getDisplayName()
                    + " is set to true, the entire subdirectory tree will be searched for files that match "
                    + "the regular expression defined in " + FILE_FILTER.getDisplayName() + "."
    ),

    FILTER_MODE_FULL_PATH(
            "filter-mode-full-path",
            "Full Path",
            "Filtering will be applied by evaluating the regular expression defined in " + FILE_FILTER.getDisplayName()
                    + " against the full path of files with and without the scheme and authority.  If "
                    + RECURSE_SUBDIRS.getDisplayName() + " is set to true, the entire subdirectory tree will be searched for files in which the full path of "
                    + "the file matches the regular expression defined in " + FILE_FILTER.getDisplayName() + ".  See 'Additional Details' for more information."
    );

    private final String value;
    private final String displayName;
    private final String description;

    FilterMode(final String value, final String displayName, final String description) {
        this.value = value;
        this.displayName = displayName;
        this.description = description;
    }

    @Override
    public String getValue() {
        return value;
    }

    @Override
    public String getDisplayName() {
        return displayName;
    }

    @Override
    public String getDescription() {
        return description;
    }

    public static FilterMode forName(String filterMode) {
        return Stream.of(values())
                .filter(fm -> fm.getValue().equalsIgnoreCase(filterMode))
                .findFirst()
                .orElseThrow(
                        () -> new IllegalArgumentException("Invalid filter mode: " + filterMode));
    }
}
