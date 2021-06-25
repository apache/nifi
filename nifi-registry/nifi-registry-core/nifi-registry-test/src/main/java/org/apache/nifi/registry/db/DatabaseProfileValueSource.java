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
package org.apache.nifi.registry.db;

import org.springframework.test.annotation.ProfileValueSource;

/**
 * This {@link ProfileValueSource} offers a set of keys  {@code current.database.is.<database>} and
 * {@code current.database.is.not.<database>} where {@code <database> } is a database as used in active profiles
 * to enable integration tests to run with a certain database. The value returned for these keys is
 * {@code "true"} or {@code "false"} depending on if the database is actually the one currently used by integration tests.
 *
 * @author Jens Schauder
 */
public class DatabaseProfileValueSource implements ProfileValueSource {

    private static final String MYSQL = "mysql";
    private static final String MARIADB = "mariadb";
    private static final String POSTGRES = "postgres";
    private static final String H2 = "h2";

    private String currentDatabase;

    DatabaseProfileValueSource() {
        final String activeProfiles = System.getProperty("spring.profiles.active", H2);

        if (activeProfiles.contains(H2)) {
            currentDatabase = H2;
        } else if (activeProfiles.contains(MYSQL)) {
            currentDatabase = MYSQL;
        } else if (activeProfiles.contains(MARIADB)) {
            currentDatabase = MARIADB;
        } else if (activeProfiles.contains(POSTGRES)) {
            currentDatabase = POSTGRES;
        }
    }

    @Override
    public String get(String key) {
        if (!key.startsWith("current.database.is.")) {
            return null;
        }
        if (key.startsWith("current.database.is.not.")) {
            return Boolean.toString(!key.endsWith(currentDatabase)).toLowerCase();
        }
        return Boolean.toString(key.endsWith(currentDatabase)).toLowerCase();
    }
}
