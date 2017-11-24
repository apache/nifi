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
     * Classpath list separated by semicolon. You can use masks like `*`, `*.jar` in file name.
     *
     * @return file list defined by classpath parameter
     */
    public static Set<File> listPathsFiles(String classpath) {
        if (classpath == null || classpath.length() == 0) {
            return Collections.emptySet();
        }
        Set<File> files = new HashSet<>();
        for (String cp : classpath.split("\\s*;\\s*")) {
            files.addAll(listPathFiles(cp));
        }
        return files;
    }

    /**
     * returns file list from one path. the path could be exact filename (one file returned), exact directory (all files from dir returned)
     * or exact dir with masked file names like ./dir/*.jar (all jars returned)
     */
    public static List<File> listPathFiles(String path) {
        File f = new File(path);
        String fname = f.getName();
        if (fname.contains("?") || fname.contains("*")) {
            Pattern pattern = Pattern.compile(fname.replace(".", "\\.").replace("?", ".?").replace("*", ".*?"));
            File[] list = f.getParentFile().listFiles((dir, name) -> pattern.matcher(name).find());
            return list==null ? Collections.emptyList() : Arrays.asList(list);
        }
        if (!f.exists()) {
            System.err.println("WARN: path not found for: " + f);
        }
        return Arrays.asList(f);
    }
}
