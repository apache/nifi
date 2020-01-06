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
package org.apache.nifi.accumulo.data;

/**
 * Encapsulates configuring the session with some required parameters.
 *
 * Justification: Generally not a fan of this fluent API to configure other objects, but there is a lot encapsulated here
 * so it helps minimize what we pass between the current set of classes and the upcoming features.
 */
public class AccumuloRecordConfiguration {
    private String tableName;
    private String rowFieldName;
    private String columnFamily;
    private String columnFamilyField;
    private String timestampField;
    private String fieldDelimiter;
    private boolean encodeFieldDelimiter;
    private boolean qualifierInKey;
    private boolean deleteKeys;


    protected AccumuloRecordConfiguration(final String tableName, final String rowFieldName, final String columnFamily,
                                          final String columnFamilyField,
                                          final String timestampField, final String fieldDelimiter,
                                          final boolean encodeFieldDelimiter,
                                          final boolean qualifierInKey, final boolean deleteKeys) {
        this.tableName = tableName;
        this.rowFieldName = rowFieldName;
        this.columnFamily = columnFamily;
        this.columnFamilyField = columnFamilyField;
        this.timestampField = timestampField;
        this.fieldDelimiter = fieldDelimiter;
        this.encodeFieldDelimiter = encodeFieldDelimiter;
        this.qualifierInKey = qualifierInKey;
        this.deleteKeys = deleteKeys;
    }

    public String getTableName(){
        return tableName;
    }

    public String getColumnFamily() {
        return columnFamily;
    }

    public String getColumnFamilyField() {
        return columnFamilyField;
    }

    public boolean getEncodeDelimiter(){
        return encodeFieldDelimiter;
    }

    public String getTimestampField(){

        return timestampField;
    }

    public String getFieldDelimiter(){
        return fieldDelimiter;
    }

    public boolean getQualifierInKey(){
        return qualifierInKey;
    }

    public boolean isDeleteKeys(){
        return deleteKeys;
    }


    public String getRowField(){
        return rowFieldName;
    }

    public static class Builder{

        public static final Builder newBuilder(){
            return new Builder();
        }

        public Builder setRowField(final String rowFieldName){
            this.rowFieldName = rowFieldName;
            return this;
        }

        public Builder setTableName(final String tableName){
            this.tableName = tableName;
            return this;
        }

        public Builder setEncodeFieldDelimiter(final boolean encodeFieldDelimiter){
            this.encodeFieldDelimiter = encodeFieldDelimiter;
            return this;
        }


        public Builder setColumnFamily(final String columnFamily){
            this.columnFamily = columnFamily;
            return this;
        }

        public Builder setColumnFamilyField(final String columnFamilyField){
            this.columnFamilyField = columnFamilyField;
            return this;
        }

        public Builder setTimestampField(final String timestampField){
            this.timestampField = timestampField;
            return this;
        }

        public Builder setQualifierInKey(final boolean qualifierInKey){
            this.qualifierInKey = qualifierInKey;
            return this;
        }

        public Builder setFieldDelimiter(final String fieldDelimiter){
            this.fieldDelimiter = fieldDelimiter;
            return this;
        }

        public Builder setDelete(final boolean deleteKeys){
            this.deleteKeys = deleteKeys;
            return this;
        }

        public AccumuloRecordConfiguration build(){
            return new AccumuloRecordConfiguration(tableName,rowFieldName,columnFamily,columnFamilyField,timestampField,fieldDelimiter,encodeFieldDelimiter,qualifierInKey,deleteKeys);
        }


        private String tableName;
        private String rowFieldName;
        private String columnFamily;
        private String columnFamilyField;
        private String fieldDelimiter;
        private boolean qualifierInKey=false;
        private boolean encodeFieldDelimiter=false;
        private String timestampField;
        private boolean deleteKeys=false;
    }
}
