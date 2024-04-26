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

/**
 * Encapsulates the information for a delete operation.
 */
public class DeleteRequest {
    private byte[] rowId;
    private byte[] columnFamily;
    private byte[] columnQualifier;
    private String visibilityLabel;

    public DeleteRequest(byte[] rowId, byte[] columnFamily, byte[] columnQualifier, String visibilityLabel) {
        this.rowId = rowId;
        this.columnFamily = columnFamily;
        this.columnQualifier = columnQualifier;
        this.visibilityLabel = visibilityLabel;
    }

    public byte[] getRowId() {
        return rowId;
    }

    public byte[] getColumnFamily() {
        return columnFamily;
    }

    public byte[] getColumnQualifier() {
        return columnQualifier;
    }

    public String getVisibilityLabel() {
        return visibilityLabel;
    }

    @Override
    public String toString() {
        return new StringBuilder()
            .append(String.format("Row ID: %s\n", new String(rowId)))
            .append(String.format("Column Family: %s\n", new String(columnFamily)))
            .append(String.format("Column Qualifier: %s\n", new String(columnQualifier)))
            .append(visibilityLabel != null ? String.format("Visibility Label: %s", visibilityLabel) : "")
            .toString();
    }
}
