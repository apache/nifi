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
package org.apache.nifi.hbase;

import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.controller.ControllerService;
import org.apache.nifi.hbase.put.PutColumn;
import org.apache.nifi.hbase.put.PutFlowFile;
import org.apache.nifi.hbase.scan.Column;
import org.apache.nifi.hbase.scan.ResultHandler;

import java.io.IOException;
import java.util.Collection;
import java.util.List;

@Tags({"hbase", "client"})
@CapabilityDescription("A controller service for accessing an HBase client.")
public interface HBaseClientService extends ControllerService {

    /**
     * Puts a batch of mutations to the given table.
     *
     * @param tableName the name of an HBase table
     * @param puts a list of put mutations for the given table
     * @throws IOException thrown when there are communication errors with HBase
     */
    void put(String tableName, Collection<PutFlowFile> puts) throws IOException;

    /**
     * Puts the given row to HBase with the provided columns.
     *
     * @param tableName the name of an HBase table
     * @param rowId the id of the row to put
     * @param columns the columns of the row to put
     * @throws IOException thrown when there are communication errors with HBase
     */
    void put(String tableName, byte[] rowId, Collection<PutColumn> columns) throws IOException;

    /**
     * Atomically checks if a row/family/qualifier value matches the expected value. If it does, then the Put is added to HBase.
     *
     * @param tableName the name of an HBase table
     * @param rowId the id of the row to check
     * @param family the family of the row to check
     * @param qualifier the qualifier of the row to check
     * @param value the value of the row to check. If null, the check is for the lack of column (ie: non-existence)
     * @return True if the Put was executed, false otherwise
     * @throws IOException thrown when there are communication errors with HBase$
     */
    boolean checkAndPut(String tableName, byte[] rowId, byte[] family, byte[] qualifier, byte[] value, PutColumn column) throws IOException;

    /**
     * Deletes the given row on HBase. All cells are deleted.
     *
     * @param tableName the name of an HBase table
     * @param rowId the id of the row to delete
     * @throws IOException thrown when there are communication errors with HBase
     */
    void delete(String tableName, byte[] rowId) throws IOException;

    /**
     * Deletes the given row on HBase. Uses the supplied visibility label for all cells in the delete.
     * It will fail if HBase cannot delete a cell because the visibility label on the cell does not match the specified
     * label.
     *
     * @param tableName the name of an HBase table
     * @param rowId the id of the row to delete
     * @param visibilityLabel a visibility label to apply to the delete
     * @throws IOException thrown when there are communication errors with HBase
     */
    void delete(String tableName, byte[] rowId, String visibilityLabel) throws IOException;

    /**
     * Deletes a list of rows in HBase. All cells are deleted.
     *
     * @param tableName the name of an HBase table
     * @param rowIds a list of rowIds to send in a batch delete
     */

    void delete(String tableName, List<byte[]> rowIds) throws IOException;

    /**
     * Deletes a list of cells from HBase. This is intended to be used with granular delete operations.
     *
     * @param tableName the name of an HBase table.
     * @param deletes a list of DeleteRequest objects.
     * @throws IOException thrown when there are communication errors with HBase
     */
    void deleteCells(String tableName, List<DeleteRequest> deletes) throws IOException;

    /**
     * Deletes a list of rows in HBase. All cells that match the visibility label are deleted.
     *
     * @param tableName the name of an HBase table
     * @param rowIds a list of rowIds to send in a batch delete
     * @param visibilityLabel a visibility label expression
     */

    void delete(String tableName, List<byte[]> rowIds, String visibilityLabel) throws IOException;

    /**
     * Scans the given table using the optional filter criteria and passing each result to the provided handler.
     *
     * @param tableName the name of an HBase table to scan
     * @param columns optional columns to return, if not specified all columns are returned
     * @param filterExpression optional filter expression, if not specified no filtering is performed
     * @param minTime the minimum timestamp of cells to return, passed to the HBase scanner timeRange
     * @param handler a handler to process rows of the result set
     * @throws IOException thrown when there are communication errors with HBase
     */
    void scan(String tableName, Collection<Column> columns, String filterExpression, long minTime, ResultHandler handler) throws IOException;

    /**
     * Scans the given table using the optional filter criteria and passing each result to the provided handler.
     *
     * @param tableName the name of an HBase table to scan
     * @param columns optional columns to return, if not specified all columns are returned
     * @param filterExpression optional filter expression, if not specified no filtering is performed
     * @param minTime the minimum timestamp of cells to return, passed to the HBase scanner timeRange
     * @param authorizations the visibility labels to apply to the scanner.
     * @param handler a handler to process rows of the result set
     * @throws IOException thrown when there are communication errors with HBase
     */
    void scan(String tableName, Collection<Column> columns, String filterExpression, long minTime, List<String> authorizations, ResultHandler handler) throws IOException;

    /**
     * Scans the given table for the given rowId and passes the result to the handler.
     *
     * @param tableName the name of an HBase table to scan
     * @param startRow the row identifier to start scanning at
     * @param endRow the row identifier to end scanning at
     * @param columns optional columns to return, if not specified all columns are returned
     * @param handler a handler to process rows of the result
     * @throws IOException thrown when there are communication errors with HBase
     */
    void scan(String tableName, byte[] startRow, byte[] endRow, Collection<Column> columns, List<String> authorizations, ResultHandler handler) throws IOException;

    /**
     * Scans the given table for the given range of row keys or time rage and passes the result to a handler.<br/>
     *
     * @param tableName the name of an HBase table to scan
     * @param startRow the row identifier to start scanning at
     * @param endRow the row identifier to end scanning at
     * @param filterExpression  optional filter expression, if not specified no filtering is performed
     * @param timerangeMin the minimum timestamp of cells to return, passed to the HBase scanner timeRange
     * @param timerangeMax the maximum timestamp of cells to return, passed to the HBase scanner timeRange
     * @param limitRows the maximum number of rows to be returned by scanner
     * @param isReversed whether this scan is a reversed one.
     * @param columns optional columns to return, if not specified all columns are returned
     * @param authorizations optional list of visibility labels that the user should be able to see when communicating with HBase
     * @param handler a handler to process rows of the result
     */
    void scan(String tableName, String startRow, String endRow, String filterExpression, Long timerangeMin, Long timerangeMax, Integer limitRows,
            Boolean isReversed, Collection<Column> columns, List<String> authorizations, ResultHandler handler) throws IOException;

    /**
     * Converts the given boolean to it's byte representation.
     *
     * @param b a boolean
     * @return the boolean represented as bytes
     */
    byte[] toBytes(boolean b);

    /**
     * Converts the given float to its byte representation.
     *
     * @param f a float
     * @return the float represented as bytes
     */
    byte[] toBytes(float f);


    /**
     * Converts the given float to its byte representation.
     *
     * @param i an int
     * @return the int represented as bytes
     */
    byte[] toBytes(int i);

    /**
     * Converts the given long to it's byte representation.
     *
     * @param l a long
     * @return the long represented as bytes
     */
    byte[] toBytes(long l);

    /**
     * Converts the given double to it's byte representation.
     *
     * @param d a double
     * @return the double represented as bytes
     */
    byte[] toBytes(double d);

    /**
     * Converts the given string to it's byte representation.
     *
     * @param s a string
     * @return the string represented as bytes
     */
    byte[] toBytes(String s);

    /**
     * Converts the given binary formatted string to a byte representation
     * @param s a binary encoded string
     * @return the string represented as bytes
     */
    byte[] toBytesBinary(String s);

    /**
     * Create a transit URI from the current configuration and the specified table name.
     * The default implementation just prepend "hbase://" to the table name and row key, i.e. "hbase://tableName/rowKey".
     * @param tableName The name of a HBase table
     * @param rowKey The target HBase row key, this can be null or empty string if the operation is not targeted to a specific row
     * @return a qualified transit URI which can identify a HBase table row in a HBase cluster
     */
    default String toTransitUri(String tableName, String rowKey) {
        return "hbase://" + tableName + (rowKey != null && !rowKey.isEmpty() ? "/" + rowKey : "");
    }

}
