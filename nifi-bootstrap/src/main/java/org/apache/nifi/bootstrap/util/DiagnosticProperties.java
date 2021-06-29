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
package org.apache.nifi.bootstrap.util;

import org.apache.nifi.properties.BootstrapProperties;
import org.apache.nifi.util.NiFiBootstrapUtils;
import org.apache.nifi.util.file.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Comparator;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class DiagnosticProperties {

    private static final Logger logger = LoggerFactory.getLogger(DiagnosticProperties.class);

    private static final String ALLOWED_PROP_NAME = "nifi.diag.allowed";
    private static final boolean ALLOWED_DEFAULT_VALUE = true;

    private static final String DIR_PROP_NAME = "nifi.diag.dir";
    private static final String DIR_DEFAULT_VALUE = "./diagnostics";

    private static final String MAX_FILE_COUNT_PROP_NAME = "nifi.diag.filecount.max";
    private static final int MAX_FILE_COUNT_DEFAULT_VALUE = Integer.MAX_VALUE;

    private static final String MAX_SIZE_PROP_NAME = "nifi.diag.size.max.byte";
    private static final int MAX_SIZE_DEFAULT_VALUE = Integer.MAX_VALUE;

    private static final String VERBOSE_PROP_NAME = "nifi.diag.verbose";
    private static final boolean VERBOSE_DEFAULT_VALUE = false;

    private final String dirPath;
    private final int maxFileCount;
    private final int maxSizeInBytes;
    private final boolean verbose;
    private final boolean allowed;

    public DiagnosticProperties() throws IOException {
        BootstrapProperties properties = NiFiBootstrapUtils.loadBootstrapProperties();
        this.dirPath = properties.getProperty(DIR_PROP_NAME, DIR_DEFAULT_VALUE);
        this.maxFileCount = getPropertyAsInt(properties.getProperty(MAX_FILE_COUNT_PROP_NAME), MAX_FILE_COUNT_DEFAULT_VALUE);
        this.maxSizeInBytes = getPropertyAsInt(properties.getProperty(MAX_SIZE_PROP_NAME), MAX_SIZE_DEFAULT_VALUE);
        this.verbose = getPropertyAsBoolean(properties.getProperty(VERBOSE_PROP_NAME), VERBOSE_DEFAULT_VALUE);
        this.allowed = getPropertyAsBoolean(properties.getProperty(ALLOWED_PROP_NAME), ALLOWED_DEFAULT_VALUE);
        createDiagDir();
    }

    public Path getOldestFile() throws IOException {
        Comparator<? super Path> lastModifiedComparator = Comparator.comparingLong(p -> p.toFile().lastModified());

        final List<Path> oldestFiles;

        try (Stream<Path> paths = Files.walk(Paths.get(dirPath))) {
            oldestFiles = paths.filter(Files::isRegularFile)
                    .sorted(lastModifiedComparator)
                    .limit(1)
                    .collect(Collectors.toList());
        }

        return oldestFiles.get(0);
    }

    public boolean isFileCountExceeded() {
        return new File(dirPath).list().length >= maxFileCount;
    }

    public boolean isSizeExceeded() {
        return FileUtils.getDirectorySize(Paths.get(dirPath), logger) >= maxSizeInBytes;
    }

    public boolean isVerbose() {
        return verbose;
    }

    public String getDirPath() {
        return dirPath;
    }

    public boolean isAllowed() {
        return allowed;
    }

    private static int getPropertyAsInt(final String property, final int defaultValue) {
        try {
            return Integer.parseInt(property);
        } catch (NumberFormatException e) {
            return defaultValue;
        }
    }

    private static boolean getPropertyAsBoolean(final String property, final boolean defaultValue) {
        try {
            return Boolean.parseBoolean(property);
        } catch (NumberFormatException e) {
            return defaultValue;
        }
    }

    private void createDiagDir() {
        File file = new File(dirPath);
        file.mkdir();
    }
}
