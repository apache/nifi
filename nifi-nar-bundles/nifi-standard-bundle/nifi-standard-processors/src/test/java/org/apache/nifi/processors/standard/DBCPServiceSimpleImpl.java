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
package org.apache.nifi.processors.standard;

import org.apache.nifi.controller.AbstractControllerService;
import org.apache.nifi.dbcp.DBCPService;
import org.apache.nifi.processor.exception.ProcessException;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.DriverManager;

/**
 * Simple implementation only for DB processor testing.
 */
public class DBCPServiceSimpleImpl extends AbstractControllerService implements DBCPService {

    private String databaseLocation;
    private boolean isDerby;

    // Default to use Derby connection
    public DBCPServiceSimpleImpl(final String databaseLocation) {
        this(databaseLocation, true);
    }

    public DBCPServiceSimpleImpl(final String databaseLocation, final boolean isDerby) {
        this.databaseLocation = databaseLocation;
        this.isDerby = isDerby;
    }

    @Override
    public String getIdentifier() {
        return "dbcp";
    }

    @Override
    public Connection getConnection() throws ProcessException {
        try {
            if (isDerby) {
                Class.forName("org.apache.derby.jdbc.EmbeddedDriver");
                return DriverManager.getConnection("jdbc:derby:" + databaseLocation + ";create=true");
            } else {
                // Use H2
                Path currentPath = Paths.get("");
                String absolutePathPrefix = currentPath.toFile().getAbsolutePath();
                String connectionString = "jdbc:h2:file:" + absolutePathPrefix + "/" + databaseLocation + ";DB_CLOSE_ON_EXIT=TRUE";
                return DriverManager.getConnection(connectionString, "SA", "");
            }
        } catch (final Exception e) {
            throw new ProcessException("getConnection failed: " + e);
        }
    }
}