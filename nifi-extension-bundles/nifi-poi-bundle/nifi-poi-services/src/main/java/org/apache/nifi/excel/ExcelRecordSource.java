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

import org.apache.commons.lang3.math.NumberUtils;
import org.apache.nifi.context.PropertyContext;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.schema.inference.RecordSource;
import org.apache.poi.ss.usermodel.Row;

import java.io.InputStream;
import java.util.List;
import java.util.Map;

public class ExcelRecordSource implements RecordSource<Row> {
    private final RowIterator rowIterator;

    public ExcelRecordSource(final InputStream in, final PropertyContext context, final Map<String, String> variables, final ComponentLog logger) {
        final String requiredSheetsDelimited = context.getProperty(ExcelReader.REQUIRED_SHEETS).evaluateAttributeExpressions(variables).getValue();
        final List<String> requiredSheets = ExcelReader.getRequiredSheets(requiredSheetsDelimited);
        final Integer rawFirstRow = context.getProperty(ExcelReader.STARTING_ROW).evaluateAttributeExpressions(variables).asInteger();
        final int firstRow = rawFirstRow == null ? NumberUtils.toInt(ExcelReader.STARTING_ROW.getDefaultValue()) : rawFirstRow;
        final int zeroBasedFirstRow = ExcelReader.getZeroBasedIndex(firstRow);
        final String password = context.getProperty(ExcelReader.PASSWORD).getValue();
        final ExcelRecordReaderConfiguration configuration = new ExcelRecordReaderConfiguration.Builder()
                .withRequiredSheets(requiredSheets)
                .withFirstRow(zeroBasedFirstRow)
                .withPassword(password)
                .build();
        this.rowIterator = new RowIterator(in, configuration, logger);
    }

    @Override
    public Row next() {
        return rowIterator.hasNext() ? rowIterator.next() : null;
    }
}
