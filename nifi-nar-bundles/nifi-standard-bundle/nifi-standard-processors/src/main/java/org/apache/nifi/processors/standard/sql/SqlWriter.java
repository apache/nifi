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

import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processors.standard.AbstractQueryDatabaseTable;

import java.io.IOException;
import java.io.OutputStream;
import java.sql.ResultSet;
import java.util.Collections;
import java.util.Map;

/**
 * The SqlWriter interface provides a standard way for processors such as ExecuteSQL, ExecuteSQLRecord, QueryDatabaseTable, and QueryDatabaseTableRecord
 * to write SQL result sets out to a flow file in whichever manner is appropriate. For example, ExecuteSQL writes the result set as Avro but ExecuteSQLRecord
 * uses the Record API to write the result set out as prescribed by the selected RecordSetWriter.
 */
public interface SqlWriter {

    /**
     * Writes the given result set out to the given output stream, possibly applying a callback as each row is processed.
     * @param resultSet the ResultSet to be written
     * @param outputStream the OutputStream to write the result set to
     * @param logger a common logger that can be used to log messages during write
     * @param callback a MaxValueResultSetRowCollector that may be called as each row in the ResultSet is processed
     * @return the number of rows written to the output stream
     * @throws Exception if any errors occur during the writing of the result set to the output stream
     */
    long writeResultSet(ResultSet resultSet, OutputStream outputStream, ComponentLog logger, AbstractQueryDatabaseTable.MaxValueResultSetRowCollector callback) throws Exception;

    /**
     * Returns a map of attribute key/value pairs to be added to any outgoing flow file(s). The default implementation is to return an empty map.
     * @return a map of attribute key/value pairs
     */
    default Map<String, String> getAttributesToAdd() {
        return Collections.emptyMap();
    }

    /**
     * Updates any session counters as a result of processing result sets. The default implementation is empty, no counters will be updated.
     * @param session the session upon which to update counters
     */
    default void updateCounters(ProcessSession session) {
    }

    /**
     * Writes an empty result set to the output stream. In some cases a ResultSet might not have any viable rows, but will throw an error or
     * behave unexpectedly if rows are attempted to be retrieved. This method indicates the implementation should write whatever output is
     * appropriate for a result set with no rows.
     * @param outputStream the OutputStream to write the empty result set to
     * @param logger a common logger that can be used to log messages during write
     * @throws IOException if any errors occur during the writing of an empty result set to the output stream
     */
    void writeEmptyResultSet(OutputStream outputStream, ComponentLog logger) throws IOException;

    /**
     * Returns the MIME type of the output format. This can be used in FlowFile attributes or to perform format-specific processing as necessary.
     * @return the MIME type string of the output format.
     */
    String getMimeType();
}