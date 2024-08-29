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
package org.apache.nifi.bootstrap.configuration;

/**
 * Enumeration of supported bootstrap properties for the application
 */
public enum BootstrapProperty {
    CONFIGURATION_DIRECTORY("conf.dir"),

    GRACEFUL_SHUTDOWN_SECONDS("graceful.shutdown.seconds"),

    JAVA_ARGUMENT("java.arg"),

    LIBRARY_DIRECTORY("lib.dir"),

    MANAGEMENT_SERVER_ADDRESS("management.server.address"),

    WORKING_DIRECTORY("working.dir");

    private final String property;

    BootstrapProperty(final String property) {
        this.property = property;
    }

    public String getProperty() {
        return property;
    }
}
