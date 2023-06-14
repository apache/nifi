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
 *
 */

package org.apache.nifi.registry.db;

import java.sql.Connection;
import java.sql.SQLException;
import javax.sql.DataSource;
import org.flywaydb.core.internal.database.DatabaseType;
import org.flywaydb.core.internal.database.DatabaseTypeRegister;

public final class DataSourceUtils {

    private DataSourceUtils() {

    }

    /**
     * Determines the database type from the given data source.
     *
     * @param dataSource the data source
     * @return the database type
     */
    public static DatabaseType getDatabaseType(final DataSource dataSource) throws SQLException {
        try (final Connection connection = dataSource.getConnection()) {
            return DatabaseTypeRegister.getDatabaseTypeForConnection(connection);
        }
    }
}
