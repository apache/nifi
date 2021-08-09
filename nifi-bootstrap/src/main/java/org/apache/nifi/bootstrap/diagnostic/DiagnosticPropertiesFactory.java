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
package org.apache.nifi.bootstrap.diagnostic;

import java.util.Properties;

public class DiagnosticPropertiesFactory {

    private DiagnosticPropertiesFactory() {
        // factory class, not meant to be instantiated
    }

    private static final String ALLOWED_PROP_NAME = "nifi.diag.allowed";

    private static final String DIR_PROP_NAME = "nifi.diag.dir";
    private static final String DIR_DEFAULT_VALUE = "./diagnostics";

    private static final String MAX_FILE_COUNT_PROP_NAME = "nifi.diag.filecount.max";
    private static final int MAX_FILE_COUNT_DEFAULT_VALUE = 10;

    private static final String MAX_SIZE_PROP_NAME = "nifi.diag.size.max.byte";
    private static final int MAX_SIZE_DEFAULT_VALUE = Integer.MAX_VALUE;

    private static final String VERBOSE_PROP_NAME = "nifi.diag.verbose";

    public static DiagnosticProperties create(final Properties bootstrapProperties) {
        final String dirPath = bootstrapProperties.getProperty(DIR_PROP_NAME, DIR_DEFAULT_VALUE);
        final int maxFileCount = getPropertyAsInt(bootstrapProperties.getProperty(MAX_FILE_COUNT_PROP_NAME), MAX_FILE_COUNT_DEFAULT_VALUE);
        final int maxSizeInBytes = getPropertyAsInt(bootstrapProperties.getProperty(MAX_SIZE_PROP_NAME), MAX_SIZE_DEFAULT_VALUE);
        final boolean verbose = Boolean.parseBoolean(bootstrapProperties.getProperty(VERBOSE_PROP_NAME));
        final boolean allowed = Boolean.parseBoolean(bootstrapProperties.getProperty(ALLOWED_PROP_NAME));
        return new DiagnosticProperties(dirPath, maxFileCount, maxSizeInBytes, verbose, allowed);
    }

    private static int getPropertyAsInt(final String property, final int defaultValue) {
        try {
            return Integer.parseInt(property);
        } catch (NumberFormatException e) {
            return defaultValue;
        }
    }

}
