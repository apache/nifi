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
package org.apache.nifi.processors.standard.sql;

import org.apache.nifi.flowfile.attributes.CoreAttributes;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processors.standard.AbstractQueryDatabaseTable;
import org.apache.nifi.processors.standard.util.JdbcCommon;

import java.io.IOException;
import java.io.OutputStream;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;

public class DefaultAvroSqlWriter implements SqlWriter {

    private final JdbcCommon.AvroConversionOptions options;

    private final Map<String,String> attributesToAdd = new HashMap<String,String>() {{
        put(CoreAttributes.MIME_TYPE.key(), JdbcCommon.MIME_TYPE_AVRO_BINARY);
    }};

    public DefaultAvroSqlWriter(JdbcCommon.AvroConversionOptions options) {
        this.options = options;
    }

    @Override
    public long writeResultSet(ResultSet resultSet, OutputStream outputStream, ComponentLog logger, AbstractQueryDatabaseTable.MaxValueResultSetRowCollector callback) throws Exception {
        try {
            return JdbcCommon.convertToAvroStream(resultSet, outputStream, options, callback);
        } catch (SQLException e) {
            throw new ProcessException(e);
        }
    }

    @Override
    public Map<String, String> getAttributesToAdd() {
        return attributesToAdd;
    }

    @Override
    public void writeEmptyResultSet(OutputStream outputStream, ComponentLog logger) throws IOException {
        JdbcCommon.createEmptyAvroStream(outputStream);
    }

    @Override
    public String getMimeType() {
        return JdbcCommon.MIME_TYPE_AVRO_BINARY;
    }
}