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
import java.util.Optional;
import java.util.stream.Stream;

public class DiagnosticContext {

    private static final Logger logger = LoggerFactory.getLogger(DiagnosticContext.class);

    private static final String ALLOWED_PROP_NAME = "nifi.diag.allowed";

    private static final String DIR_PROP_NAME = "nifi.diag.dir";
    private static final String DIR_DEFAULT_VALUE = "./diagnostics";

    private static final String MAX_FILE_COUNT_PROP_NAME = "nifi.diag.filecount.max";
    private static final int MAX_FILE_COUNT_DEFAULT_VALUE = 10;

    private static final String MAX_SIZE_PROP_NAME = "nifi.diag.size.max.byte";
    private static final int MAX_SIZE_DEFAULT_VALUE = Integer.MAX_VALUE;

    private static final String VERBOSE_PROP_NAME = "nifi.diag.verbose";

    private final DiagnosticProperties diagnosticProperties;

    public DiagnosticContext() throws IOException {
        BootstrapProperties properties = NiFiBootstrapUtils.loadBootstrapProperties();
        final String dirPath = properties.getProperty(DIR_PROP_NAME, DIR_DEFAULT_VALUE);
        final int maxFileCount = getPropertyAsInt(properties.getProperty(MAX_FILE_COUNT_PROP_NAME), MAX_FILE_COUNT_DEFAULT_VALUE);
        final int maxSizeInBytes = getPropertyAsInt(properties.getProperty(MAX_SIZE_PROP_NAME), MAX_SIZE_DEFAULT_VALUE);
        final boolean verbose = Boolean.parseBoolean(properties.getProperty(VERBOSE_PROP_NAME));
        final boolean allowed = Boolean.parseBoolean(properties.getProperty(ALLOWED_PROP_NAME));
        diagnosticProperties = new DiagnosticProperties(dirPath, maxFileCount, maxSizeInBytes, verbose, allowed);
        createDiagnosticDirectory();
    }

    public Path getOldestFile() throws IOException {
        Comparator<? super Path> lastModifiedComparator = Comparator.comparingLong(p -> p.toFile().lastModified());

        final Optional<Path> oldestFiles;

        try (Stream<Path> paths = Files.walk(Paths.get(diagnosticProperties.getDirPath()))) {
            oldestFiles = paths
                    .filter(Files::isRegularFile)
                    .min(lastModifiedComparator);
        }

        return oldestFiles.orElseThrow(
                () -> new RuntimeException(String.format("Could not find oldest file in diagnostic directory: %s", diagnosticProperties.getDirPath())));
    }

    public boolean isFileCountExceeded() {
        return new File(diagnosticProperties.getDirPath()).list().length >= diagnosticProperties.getMaxFileCount();
    }

    public boolean isSizeExceeded() {
        return FileUtils.getDirectorySize(Paths.get(diagnosticProperties.getDirPath()), logger) >= diagnosticProperties.getMaxSizeInBytes();
    }

    public boolean isVerbose() {
        return diagnosticProperties.isVerbose();
    }

    public String getDirPath() {
        return diagnosticProperties.getDirPath();
    }

    public boolean isAllowed() {
        return diagnosticProperties.isAllowed();
    }

    private static int getPropertyAsInt(final String property, final int defaultValue) {
        try {
            return Integer.parseInt(property);
        } catch (NumberFormatException e) {
            return defaultValue;
        }
    }

    private void createDiagnosticDirectory() {
        File file = new File(diagnosticProperties.getDirPath());
        file.mkdir();
    }
}
