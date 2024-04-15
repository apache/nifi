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
package org.apache.nifi.excel;

import org.apache.nifi.serialization.record.RecordSchema;

import java.util.Collections;
import java.util.List;

public class ExcelRecordReaderConfiguration {
    private RecordSchema schema;
    private List<String> requiredSheets;
    private int firstRow;
    private String dateFormat;
    private String timeFormat;
    private String timestampFormat;
    private String password;
    private boolean avoidTempFiles;

    private ExcelRecordReaderConfiguration() {
    }

    public RecordSchema getSchema() {
        return schema;
    }

    public List<String> getRequiredSheets() {
        return requiredSheets;
    }

    public int getFirstRow() {
        return firstRow;
    }

    public String getDateFormat() {
        return dateFormat;
    }

    public String getTimeFormat() {
        return timeFormat;
    }

    public String getTimestampFormat() {
        return timestampFormat;
    }

    public String getPassword() {
        return password;
    }

    public boolean isAvoidTempFiles() {
        return avoidTempFiles;
    }

    public static final class Builder {
        private RecordSchema schema;
        private List<String> requiredSheets;
        private int firstRow;
        private String dateFormat;
        private String timeFormat;
        private String timestampFormat;
        private String password;
        private boolean avoidTempFiles;

        public Builder withSchema(RecordSchema schema) {
            this.schema = schema;
            return this;
        }

        public Builder withRequiredSheets(List<String> requiredSheets) {
            this.requiredSheets = requiredSheets;
            return this;
        }

        public Builder withFirstRow(int firstRow) {
            this.firstRow = firstRow;
            return this;
        }

        public Builder withDateFormat(String dateFormat) {
            this.dateFormat = dateFormat;
            return this;
        }

        public Builder withTimeFormat(String timeFormat) {
            this.timeFormat = timeFormat;
            return this;
        }

        public Builder withTimestampFormat(String timestampFormat) {
            this.timestampFormat = timestampFormat;
            return this;
        }

        public Builder withPassword(String password) {
            this.password = password;
            return this;
        }

        public Builder withAvoidTempFiles(boolean avoidTempFiles) {
            this.avoidTempFiles = avoidTempFiles;
            return this;
        }

        public ExcelRecordReaderConfiguration build() {
            ExcelRecordReaderConfiguration excelRecordReaderConfiguration = new ExcelRecordReaderConfiguration();
            excelRecordReaderConfiguration.schema = this.schema;
            excelRecordReaderConfiguration.timeFormat = this.timeFormat;
            excelRecordReaderConfiguration.timestampFormat = this.timestampFormat;
            excelRecordReaderConfiguration.requiredSheets = this.requiredSheets == null ? Collections.emptyList() : this.requiredSheets;
            excelRecordReaderConfiguration.dateFormat = this.dateFormat;
            excelRecordReaderConfiguration.firstRow = this.firstRow;
            excelRecordReaderConfiguration.password = password;
            excelRecordReaderConfiguration.avoidTempFiles = avoidTempFiles;

            return excelRecordReaderConfiguration;
        }
    }
}

