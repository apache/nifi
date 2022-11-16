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

/**
 * Java Runtime Version Provider with information on supported and deprecated versions
 */
public class RuntimeVersionProvider {

    private static final String JAVA_VERSION_PROPERTY = "java.version";

    private static final int DEPRECATED_JAVA_VERSION = 8;

    private static final int MINIMUM_JAVA_VERSION = 11;

    /**
     * Get major version from java.version System property
     *
     * @return Major Version
     */
    public static int getMajorVersion() {
        final String javaVersion = System.getProperty(JAVA_VERSION_PROPERTY);
        return OSUtils.parseJavaVersion(javaVersion);
    }

    /**
     * Get minimum supported major version
     *
     * @return Minimum Major Version
     */
    public static int getMinimumMajorVersion() {
        return MINIMUM_JAVA_VERSION;
    }

    /**
     * Is major version deprecated
     *
     * @param majorVersion Java major version
     * @return Deprecated status
     */
    public static boolean isMajorVersionDeprecated(final int majorVersion) {
        return majorVersion == DEPRECATED_JAVA_VERSION;
    }
}
