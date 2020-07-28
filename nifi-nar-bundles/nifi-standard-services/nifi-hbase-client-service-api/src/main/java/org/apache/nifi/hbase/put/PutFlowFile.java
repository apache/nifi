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
package org.apache.nifi.hbase.put;

import org.apache.nifi.flowfile.FlowFile;

import java.util.Collection;

/**
 * Wrapper to encapsulate all of the information for the Put along with the FlowFile.
 */
public class PutFlowFile {

    private final String tableName;
    private final byte[] row;
    private final Collection<PutColumn> columns;
    private final FlowFile flowFile;

    public PutFlowFile(String tableName, byte[] row, Collection<PutColumn> columns, FlowFile flowFile) {
        this.tableName = tableName;
        this.row = row;
        this.columns = columns;
        this.flowFile = flowFile;
    }

    public String getTableName() {
        return tableName;
    }

    public byte[] getRow() {
        return row;
    }

    public Collection<PutColumn> getColumns() {
        return columns;
    }

    public FlowFile getFlowFile() {
        return flowFile;
    }

    public boolean isValid() {
        if (tableName == null || tableName.trim().isEmpty() || null == row || flowFile == null || columns == null || columns.isEmpty()) {
            return false;
        }

        for (PutColumn column : columns) {
            if (null == column.getColumnQualifier() || null == column.getColumnFamily() || column.getBuffer() == null) {
                return false;
            }
        }

        return true;
    }

    @Override
    public boolean equals(Object obj) {
        if (obj instanceof PutFlowFile) {
            PutFlowFile pff = (PutFlowFile)obj;
            return this.tableName.equals(pff.tableName)
                    && this.row.equals(pff.row)
                    && this.columns.equals(pff.columns)
                    && this.flowFile.equals(pff.flowFile);
        } else {
            return false;
        }
    }
}
