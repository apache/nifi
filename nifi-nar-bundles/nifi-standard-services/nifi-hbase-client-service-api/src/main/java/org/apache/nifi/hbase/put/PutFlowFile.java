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

/**
 * Wrapper to encapsulate all of the information for the Put along with the FlowFile.
 */
public class PutFlowFile {

    private final String tableName;
    private final String row;
    private final String columnFamily;
    private final String columnQualifier;
    private final byte[] buffer;
    private final FlowFile flowFile;

    public PutFlowFile(String tableName, String row, String columnFamily, String columnQualifier,
                       byte[] buffer, FlowFile flowFile) {
        this.tableName = tableName;
        this.row = row;
        this.columnFamily = columnFamily;
        this.columnQualifier = columnQualifier;
        this.buffer = buffer;
        this.flowFile = flowFile;
    }

    public String getTableName() {
        return tableName;
    }

    public String getRow() {
        return row;
    }

    public String getColumnFamily() {
        return columnFamily;
    }

    public String getColumnQualifier() {
        return columnQualifier;
    }

    public byte[] getBuffer() {
        return buffer;
    }

    public FlowFile getFlowFile() {
        return flowFile;
    }

}
