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
package org.apache.nifi.h2.database.migration;

import java.sql.Driver;

/**
 * H2 Database Version description
 */
enum DatabaseVersion {
    UNKNOWN("0", 0, false, new org.h2.Driver()),

    VERSION_1_4("1.4", 1, false, new v14.h2.Driver()),

    VERSION_2_1("2.1", 2, true, new v21.h2.Driver()),

    VERSION_2_2("2.2", 3, true, new org.h2.Driver());

    private final String version;

    private final int formatVersion;

    private final Driver driver;

    private final String packageName;

    private final boolean repackagingRequired;

    DatabaseVersion(final String version, final int formatVersion, final boolean repackagingRequired, final Driver driver) {
        this.version = version;
        this.formatVersion = formatVersion;
        this.repackagingRequired = repackagingRequired;
        this.driver = driver;
        this.packageName = driver.getClass().getName().replace(".Driver", "");
    }

    String getVersion() {
        return version;
    }

    int getFormatVersion() {
        return formatVersion;
    }

    boolean isRepackagingRequired() {
        return repackagingRequired;
    }

    String getPackageName() {
        return packageName;
    }

    Driver getDriver() {
        return driver;
    }
}
