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
package org.apache.nifi.cdc.event;

import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.nifi.distributed.cache.client.exception.SerializationException;

import java.io.IOException;
import java.io.OutputStream;

import static org.apache.nifi.cdc.event.TableInfo.DB_TABLE_NAME_DELIMITER;

/**
 * This class represents a key in a cache that contains information (column definitions, e.g.) for a database table
 */
public class TableInfoCacheKey {

    private final String databaseName;
    private final String tableName;
    private final long tableId;
    private final String uuidPrefix;

    public TableInfoCacheKey(String uuidPrefix, String databaseName, String tableName, long tableId) {
        this.uuidPrefix = uuidPrefix;
        this.databaseName = databaseName;
        this.tableName = tableName;
        this.tableId = tableId;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        TableInfoCacheKey that = (TableInfoCacheKey) o;

        return new EqualsBuilder()
                .append(tableId, that.tableId)
                .append(databaseName, that.databaseName)
                .append(tableName, that.tableName)
                .append(uuidPrefix, that.uuidPrefix)
                .isEquals();
    }

    @Override
    public int hashCode() {
        int result = databaseName != null ? databaseName.hashCode() : 0;
        result = 31 * result + (tableName != null ? tableName.hashCode() : 0);
        result = 31 * result + (int) (tableId ^ (tableId >>> 32));
        result = 31 * result + (uuidPrefix != null ? uuidPrefix.hashCode() : 0);
        return result;
    }

    public String getDatabaseName() {
        return databaseName;
    }

    public String getTableName() {
        return tableName;
    }

    public long getTableId() {
        return tableId;
    }

    public String getUuidPrefix() {
        return uuidPrefix;
    }

    public static class Serializer implements org.apache.nifi.distributed.cache.client.Serializer<TableInfoCacheKey> {

        @Override
        public void serialize(TableInfoCacheKey key, OutputStream output) throws SerializationException, IOException {
            StringBuilder sb = new StringBuilder(key.getUuidPrefix());
            sb.append(DB_TABLE_NAME_DELIMITER);
            sb.append(key.getDatabaseName());
            sb.append(DB_TABLE_NAME_DELIMITER);
            sb.append(key.getTableName());
            sb.append(DB_TABLE_NAME_DELIMITER);
            sb.append(key.getTableId());
            output.write(sb.toString().getBytes());
        }
    }
}