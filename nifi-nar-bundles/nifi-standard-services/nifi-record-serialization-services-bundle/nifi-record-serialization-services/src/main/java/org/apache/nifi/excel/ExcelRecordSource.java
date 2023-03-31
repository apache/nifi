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
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.schema.inference.RecordSource;
import org.apache.poi.ss.usermodel.Row;

import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;

public class ExcelRecordSource implements RecordSource<Row> {
    private final RowIterator rowIterator;
    public ExcelRecordSource(final InputStream in, final PropertyContext context, final Map<String, String> variables) {
        try {
            String desiredSheetsDelimited = context.getProperty(ExcelReader.DESIRED_SHEETS).evaluateAttributeExpressions(variables).getValue();
            String [] rawDesiredSheets = ExcelReader.getRawDesiredSheets(desiredSheetsDelimited, null);
            Integer rawFirstRow = context.getProperty(ExcelReader.FIRST_ROW_NUM).evaluateAttributeExpressions(variables).asInteger();
            int firstRow = rawFirstRow != null ? rawFirstRow : NumberUtils.toInt(ExcelReader.FIRST_ROW_NUM.getDefaultValue());
            firstRow = ExcelReader.getZeroBasedIndex(firstRow);
            this.rowIterator = new RowIterator(in, getDesiredSheets(rawDesiredSheets), firstRow);
        } catch (RuntimeException e) {
            throw new ProcessException(e);
        }
    }

    static List<String> getDesiredSheets(String [] rawDesiredSheets) {
        return rawDesiredSheets != null ? Arrays.asList(rawDesiredSheets) : Collections.emptyList();
    }
    @Override
    public Row next() throws IOException {
        return rowIterator.hasNext() ? rowIterator.next() : null;
    }
}
