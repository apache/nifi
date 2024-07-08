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

import com.github.pjfanning.xlsx.StreamingReader;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.ss.usermodel.Sheet;
import org.apache.poi.ss.usermodel.Workbook;

import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.stream.Collectors;

class RowIterator implements Iterator<Row>, Closeable {
    private final Workbook workbook;
    private final Iterator<Sheet> sheets;
    private final int firstRow;
    private final ComponentLog logger;
    private Sheet currentSheet;
    private Iterator<Row> currentRows;
    private Row currentRow;

    RowIterator(final InputStream in, final ExcelRecordReaderConfiguration configuration, final ComponentLog logger) {
        this.workbook = StreamingReader.builder()
                .rowCacheSize(100)
                .bufferSize(4096)
                .password(configuration.getPassword())
                .setAvoidTempFiles(configuration.isAvoidTempFiles())
                .setReadSharedFormulas(true) // NOTE: If not set to true, then data with shared formulas fail.
                .open(in);

        final List<String> requiredSheets = configuration.getRequiredSheets();
        if (requiredSheets == null || requiredSheets.isEmpty()) {
            this.sheets = this.workbook.iterator();
        } else {
            final Map<String, Integer> requiredSheetsMap = requiredSheets.stream()
                    .collect(Collectors.toMap(key -> key, this.workbook::getSheetIndex));
            final String sheetsNotFound = requiredSheetsMap.entrySet().stream()
                    .filter(entry -> entry.getValue() == -1)
                    .map(Map.Entry::getKey)
                    .collect(Collectors.joining(","));
            if (!sheetsNotFound.isEmpty()) {
                throw new ProcessException("Required Excel Sheets not found: " + sheetsNotFound);
            }

            this.sheets = requiredSheetsMap.values().stream()
                    .map(this.workbook::getSheetAt)
                    .collect(Collectors.toList()).iterator();
        }

        this.firstRow = configuration.getFirstRow();
        this.logger = logger;
        setCurrent();
    }

    @Override
    public boolean hasNext() {
        return currentRow != null;
    }

    @Override
    public Row next() {
        if (currentRow == null) {
            throw new NoSuchElementException();
        }

        final Row next = currentRow;
        setCurrent();

        return next;
    }

    @Override
    public void close() throws IOException {
        this.workbook.close();
    }

    private void setCurrent() {
        currentRow = getNextRow();
        if (currentRow != null) {
            return;
        }

        while (sheets.hasNext()) {
            currentSheet = sheets.next();
            currentRows = currentSheet.iterator();
            currentRow = getNextRow();
            if (currentRow != null) {
                return;
            }
        }
    }

    private Row getNextRow() {
        while (currentRows != null && !hasExhaustedRows()) {
            final Row tempCurrentRow = currentRows.next();
            if (tempCurrentRow.getRowNum() >= firstRow) {
                return tempCurrentRow;
            }
        }
        return null;
    }

    private boolean hasExhaustedRows() {
        final boolean exhausted = !currentRows.hasNext();
        if (exhausted) {
            logger.debug("Exhausted all rows from sheet {}", currentSheet.getSheetName());
        }
        return exhausted;
    }
}
