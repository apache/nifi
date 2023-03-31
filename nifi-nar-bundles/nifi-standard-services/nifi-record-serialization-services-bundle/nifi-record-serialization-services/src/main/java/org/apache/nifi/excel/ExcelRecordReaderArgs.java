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

import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.serialization.record.RecordSchema;

import java.io.InputStream;
import java.util.concurrent.atomic.AtomicReferenceArray;

public class ExcelRecordReaderArgs {
    private InputStream inputStream;
    private ComponentLog logger;
    private RecordSchema schema;
    private AtomicReferenceArray<String> desiredSheets;
    private int firstRow;
    private String dateFormat;
    private String timeFormat;
    private String timestampFormat;

    private ExcelRecordReaderArgs() {
    }

    public InputStream getInputStream() {
        return inputStream;
    }

    public ComponentLog getLogger() {
        return logger;
    }

    public RecordSchema getSchema() {
        return schema;
    }

    public AtomicReferenceArray<String> getDesiredSheets() {
        return desiredSheets;
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
    public static final class Builder {
        private InputStream inputStream;
        private ComponentLog logger;
        private RecordSchema schema;
        private AtomicReferenceArray<String> desiredSheets;
        private int firstRow;
        private String dateFormat;
        private String timeFormat;
        private String timestampFormat;

        public Builder withInputStream(InputStream inputStream) {
            this.inputStream = inputStream;
            return this;
        }

        public Builder withLogger(ComponentLog logger) {
            this.logger = logger;
            return this;
        }

        public Builder withSchema(RecordSchema schema) {
            this.schema = schema;
            return this;
        }

        public Builder withDesiredSheets(AtomicReferenceArray<String> desiredSheets) {
            this.desiredSheets = desiredSheets;
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

        public ExcelRecordReaderArgs build() {
            ExcelRecordReaderArgs excelRecordReaderArgs = new ExcelRecordReaderArgs();
            excelRecordReaderArgs.schema = this.schema;
            excelRecordReaderArgs.inputStream = this.inputStream;
            excelRecordReaderArgs.timeFormat = this.timeFormat;
            excelRecordReaderArgs.timestampFormat = this.timestampFormat;
            excelRecordReaderArgs.desiredSheets = this.desiredSheets;
            excelRecordReaderArgs.dateFormat = this.dateFormat;
            excelRecordReaderArgs.firstRow = this.firstRow;
            excelRecordReaderArgs.logger = this.logger;
            return excelRecordReaderArgs;
        }
    }
}

