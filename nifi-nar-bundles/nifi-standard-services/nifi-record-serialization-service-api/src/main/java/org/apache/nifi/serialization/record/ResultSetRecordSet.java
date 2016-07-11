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

package org.apache.nifi.serialization.record;

import java.io.Closeable;
import java.io.IOException;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Types;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.nifi.serialization.SimpleRecordSchema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ResultSetRecordSet implements RecordSet, Closeable {
    private static final Logger logger = LoggerFactory.getLogger(ResultSetRecordSet.class);
    private final ResultSet rs;
    private final RecordSchema schema;
    private final Set<String> rsColumnNames;

    public ResultSetRecordSet(final ResultSet rs) throws SQLException {
        this.rs = rs;
        this.schema = createSchema(rs);

        rsColumnNames = new HashSet<>();
        final ResultSetMetaData metadata = rs.getMetaData();
        for (int i = 0; i < metadata.getColumnCount(); i++) {
            rsColumnNames.add(metadata.getColumnLabel(i + 1));
        }
    }

    @Override
    public RecordSchema getSchema() {
        return schema;
    }

    @Override
    public Record next() throws IOException {
        try {
            if (rs.next()) {
                return createRecord(rs);
            }
        } catch (final SQLException e) {
            throw new IOException("Could not obtain next record from ResultSet", e);
        }

        return null;
    }

    @Override
    public void close() {
        try {
            rs.close();
        } catch (final SQLException e) {
            logger.error("Failed to close ResultSet", e);
        }
    }

    private Record createRecord(final ResultSet rs) throws SQLException {
        final Map<String, Object> values = new HashMap<>(schema.getFieldCount());

        for (final RecordField field : schema.getFields()) {
            final String fieldName = field.getFieldName();

            final Object value;
            if (rsColumnNames.contains(fieldName)) {
                value = rs.getObject(field.getFieldName());
            } else {
                value = null;
            }

            values.put(fieldName, value);
        }

        return new MapRecord(schema, values);
    }

    private static RecordSchema createSchema(final ResultSet rs) throws SQLException {
        final ResultSetMetaData metadata = rs.getMetaData();
        final int numCols = metadata.getColumnCount();
        final List<RecordField> fields = new ArrayList<>(numCols);

        for (int i = 0; i < numCols; i++) {
            final int column = i + 1;
            final int sqlType = metadata.getColumnType(column);

            final RecordFieldType fieldType = getFieldType(sqlType);
            final String fieldName = metadata.getColumnLabel(column);
            final RecordField field = new RecordField(fieldName, fieldType.getDataType());
            fields.add(field);
        }

        return new SimpleRecordSchema(fields);
    }

    private static RecordFieldType getFieldType(final int sqlType) {
        switch (sqlType) {
            case Types.ARRAY:
                return RecordFieldType.ARRAY;
            case Types.BIGINT:
            case Types.ROWID:
                return RecordFieldType.LONG;
            case Types.BINARY:
            case Types.LONGVARBINARY:
            case Types.VARBINARY:
                return RecordFieldType.ARRAY;
            case Types.BIT:
            case Types.BOOLEAN:
                return RecordFieldType.BOOLEAN;
            case Types.CHAR:
                return RecordFieldType.CHAR;
            case Types.DATE:
                return RecordFieldType.DATE;
            case Types.DECIMAL:
            case Types.DOUBLE:
            case Types.NUMERIC:
            case Types.REAL:
                return RecordFieldType.DOUBLE;
            case Types.FLOAT:
                return RecordFieldType.FLOAT;
            case Types.INTEGER:
                return RecordFieldType.INT;
            case Types.SMALLINT:
                return RecordFieldType.SHORT;
            case Types.TINYINT:
                return RecordFieldType.BYTE;
            case Types.LONGNVARCHAR:
            case Types.LONGVARCHAR:
            case Types.NCHAR:
            case Types.NULL:
            case Types.NVARCHAR:
            case Types.VARCHAR:
                return RecordFieldType.STRING;
            case Types.OTHER:
            case Types.JAVA_OBJECT:
                return RecordFieldType.RECORD;
            case Types.TIME:
            case Types.TIME_WITH_TIMEZONE:
                return RecordFieldType.TIME;
            case Types.TIMESTAMP:
            case Types.TIMESTAMP_WITH_TIMEZONE:
                return RecordFieldType.TIMESTAMP;
        }

        return RecordFieldType.STRING;
    }
}
