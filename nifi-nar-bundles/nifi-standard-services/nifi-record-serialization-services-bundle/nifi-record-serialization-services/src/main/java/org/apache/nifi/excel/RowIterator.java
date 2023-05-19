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
 */package org.apache.nifi.excel;

import com.github.pjfanning.xlsx.StreamingReader;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.ss.usermodel.Sheet;
import org.apache.poi.ss.usermodel.Workbook;

import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class RowIterator implements Iterator<Row>, Closeable {
    private final Workbook workbook;
    private final Iterator<Sheet> sheets;
    private Sheet currentSheet;
    private Iterator<Row> currentRows;
    private final Map<String, Boolean> requiredSheets;
    private final int firstRow;
    private final ComponentLog logger;
    private Row currentRow;

    public RowIterator(InputStream in, List<String> requiredSheets, int firstRow, ComponentLog logger) {
        this.workbook = StreamingReader.builder()
                .rowCacheSize(100)
                .bufferSize(4096)
                .open(in);
        this.sheets = this.workbook.iterator();
        this.requiredSheets = requiredSheets != null ? requiredSheets.stream()
                .collect(Collectors.toMap(key -> key, value -> Boolean.FALSE)) : new HashMap<>();
        this.firstRow = firstRow;
        this.logger = logger;
    }

    @Override
    public boolean hasNext() {
        setCurrent();
        boolean next = currentRow != null;
        if(!next) {
            String sheetsNotFound = getSheetsNotFound();
            if (!sheetsNotFound.isEmpty()) {
                throw new ProcessException("The following required Excel sheet(s) were not found " + sheetsNotFound);
            }
        }
        return next;
    }

    private void setCurrent() {
        currentRow = getNextRow();
        if (currentRow != null) {
            return;
        }

        currentSheet = null;
        currentRows = null;
        while (sheets.hasNext()) {
            currentSheet = sheets.next();
            if (isIterateOverAllSheets() || hasSheet(currentSheet.getSheetName())) {
                currentRows = currentSheet.iterator();
                currentRow = getNextRow();
                if (currentRow != null) {
                    return;
                }
            }
        }
    }

    private Row getNextRow() {
        while (currentRows != null && !hasExhaustedRows()) {
            Row tempCurrentRow = currentRows.next();
            if (!isSkip(tempCurrentRow)) {
                return tempCurrentRow;
            }
        }
        return null;
    }

    private boolean hasExhaustedRows() {
        boolean exhausted = !currentRows.hasNext();
        if (exhausted) {
            logger.debug("Exhausted all rows from sheet {}", currentSheet.getSheetName());
        }
        return exhausted;
    }

    private boolean isSkip(Row row) {
        return row.getRowNum() < firstRow;
    }

    private boolean isIterateOverAllSheets() {
        boolean iterateAllSheets = requiredSheets.isEmpty();
        if (iterateAllSheets) {
            logger.debug("Advanced to sheet {}", currentSheet.getSheetName());
        }
        return iterateAllSheets;
    }

    private boolean hasSheet(String name) {
        boolean sheetByName = !requiredSheets.isEmpty()
                && requiredSheets.keySet().stream()
                .anyMatch(requiredSheet -> requiredSheet.equals(name));
        if (sheetByName) {
            requiredSheets.put(name, Boolean.TRUE);
        }

        return sheetByName;
    }

    private String getSheetsNotFound() {
        return requiredSheets.entrySet().stream()
                .filter(entry -> !entry.getValue())
                .map(Map.Entry::getKey)
                .collect(Collectors.joining(","));
    }

    @Override
    public Row next() {
        return currentRow;
    }

    @Override
    public void close() throws IOException {
        this.workbook.close();
    }
}
