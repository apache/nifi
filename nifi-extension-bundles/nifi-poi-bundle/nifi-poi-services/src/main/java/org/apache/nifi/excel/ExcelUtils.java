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

import org.apache.poi.ss.usermodel.Cell;
import org.apache.poi.ss.usermodel.CellType;
import org.apache.poi.ss.usermodel.DataFormatter;
import org.apache.poi.ss.usermodel.Row;

import java.util.Locale;

public class ExcelUtils {
    static final String FIELD_NAME_PREFIX = "column_";
    private static final DataFormatter DATA_FORMATTER = new DataFormatter(Locale.getDefault());

    static {
        DATA_FORMATTER.setUseCachedValuesForFormulaCells(true);
    }

    private ExcelUtils() {
    }

    public static boolean hasCells(final Row row) {
        return row != null && row.getFirstCellNum() != -1;
    }

    public static String getFormattedCellValue(Cell cell) {
        if (cell != null) {
            //NOTE This conditional is to avoid the following:
            //java.lang.IllegalStateException: Cannot get a error value from a formula cell
            if (cell.getCellType().equals(CellType.FORMULA) && cell.getCachedFormulaResultType().equals(CellType.ERROR)) {
                return cell.getStringCellValue();
            }
            return DATA_FORMATTER.formatCellValue(cell);
        }
        return "";
    }
}
