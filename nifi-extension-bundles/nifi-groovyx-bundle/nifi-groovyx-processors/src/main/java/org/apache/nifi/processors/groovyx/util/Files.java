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
package org.apache.nifi.processors.groovyx.util;

import org.apache.nifi.logging.ComponentLog;

import java.io.File;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.regex.Pattern;

/**
 * Helpers to work with files
 */
public class Files {

    /**
     * Classpath list separated by semicolon or comma. You can use masks like `*`, `*.jar` in file name.
     *
     * @return file list defined by classpath parameter
     */
    public static Set<File> listPathsFiles(final String classpath, final ComponentLog logger) {
        if (classpath == null || classpath.isEmpty()) {
            return Collections.emptySet();
        }
        final Set<File> files = new HashSet<>();
        for (final String cp : classpath.split("\\s*[;,]\\s*")) {
            files.addAll(listPathFiles(cp, logger));
        }
        return files;
    }

    /**
     * returns file list from one path. the path could be exact filename (one file returned), exact directory (all files from dir returned)
     * or exact dir with masked file names like ./dir/*.jar (all jars returned)
     */
    public static List<File> listPathFiles(final String path, final ComponentLog logger) {
        final File f = new File(path);
        final String fname = f.getName();
        if (fname.contains("?") || fname.contains("*")) {
            final Pattern pattern = Pattern.compile(fname.replace(".", "\\.").replace("?", ".?").replace("*", ".*?"));
            final File[] list = f.getParentFile().listFiles((dir, name) -> pattern.matcher(name).find());
            return list == null ? Collections.emptyList() : Arrays.asList(list);
        }
        if (!f.exists()) {
            logger.warn("path not found for {}", f);
        }
        return List.of(f);
    }
}
