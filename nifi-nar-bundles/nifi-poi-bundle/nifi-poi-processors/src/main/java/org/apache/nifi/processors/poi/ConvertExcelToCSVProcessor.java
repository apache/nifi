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
package org.apache.nifi.processors.poi;

import com.github.pjfanning.xlsx.StreamingReader;
import com.github.pjfanning.xlsx.exceptions.OpenException;
import com.github.pjfanning.xlsx.exceptions.ParseException;
import com.github.pjfanning.xlsx.exceptions.ReadException;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVPrinter;
import org.apache.commons.io.FilenameUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.behavior.WritesAttributes;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.csv.CSVUtils;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.flowfile.attributes.CoreAttributes;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.ProcessorInitializationContext;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.poi.openxml4j.exceptions.InvalidFormatException;
import org.apache.poi.ss.usermodel.Cell;
import org.apache.poi.ss.usermodel.Sheet;
import org.apache.poi.ss.usermodel.Workbook;

import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;


@Tags({"excel", "csv", "poi"})
@CapabilityDescription("Consumes a Microsoft Excel document and converts each worksheet to csv. Each sheet from the incoming Excel " +
        "document will generate a new Flowfile that will be output from this processor. Each output Flowfile's contents will be formatted as a csv file " +
        "where the each row from the excel sheet is output as a newline in the csv file. This processor is currently only capable of processing .xlsx " +
        "(XSSF 2007 OOXML file format) Excel documents and not older .xls (HSSF '97(-2007) file format) documents. This processor also expects well formatted " +
        "CSV content and will not escape cell's containing invalid content such as newlines or additional commas.")
@WritesAttributes({@WritesAttribute(attribute = "sheetname", description = "The name of the Excel sheet that this particular row of data came from in the Excel document"),
        @WritesAttribute(attribute = "numrows", description = "The number of rows in this Excel Sheet"),
        @WritesAttribute(attribute = "sourcefilename", description = "The name of the Excel document file that this data originated from"),
        @WritesAttribute(attribute = "convertexceltocsvprocessor.error", description = "Error message that was encountered on a per Excel sheet basis. This attribute is" +
                " only populated if an error was occured while processing the particular sheet. Having the error present at the sheet level will allow for the end" +
                " user to better understand what syntax errors in their excel doc on a larger scale caused the error.")})
public class ConvertExcelToCSVProcessor
        extends AbstractProcessor {

    private static final String CSV_MIME_TYPE = "text/csv";
    public static final String SHEET_NAME = "sheetname";
    public static final String ROW_NUM = "numrows";
    public static final String SOURCE_FILE_NAME = "sourcefilename";
    private static final String DESIRED_SHEETS_DELIMITER = ",";
    private static final String UNKNOWN_SHEET_NAME = "UNKNOWN";

    public static final PropertyDescriptor DESIRED_SHEETS = new PropertyDescriptor
            .Builder().name("extract-sheets")
            .displayName("Sheets to Extract")
            .description("Comma separated list of Excel document sheet names that should be extracted from the excel document. If this property" +
                    " is left blank then all of the sheets will be extracted from the Excel document. The list of names is case in-sensitive. Any sheets not " +
                    "specified in this value will be ignored. A bulletin will be generated if a specified sheet(s) are not found.")
            .required(false)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final PropertyDescriptor ROWS_TO_SKIP = new PropertyDescriptor
            .Builder().name("excel-extract-first-row")
            .displayName("Number of Rows to Skip")
            .description("The row number of the first row to start processing."
                    + "Use this to skip over rows of data at the top of your worksheet that are not part of the dataset."
                    + "Empty rows of data anywhere in the spreadsheet will always be skipped, no matter what this value is set to.")
            .required(true)
            .defaultValue("0")
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .addValidator(StandardValidators.NON_NEGATIVE_INTEGER_VALIDATOR)
            .build();

    public static final PropertyDescriptor COLUMNS_TO_SKIP = new PropertyDescriptor
            .Builder().name("excel-extract-column-to-skip")
            .displayName("Columns To Skip")
            .description("Comma delimited list of column numbers to skip. Use the columns number and not the letter designation. "
                    + "Use this to skip over columns anywhere in your worksheet that you don't want extracted as part of the record.")
            .required(false)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final PropertyDescriptor FORMAT_VALUES = new PropertyDescriptor.Builder()
            .name("excel-format-values")
            .displayName("Format Cell Values")
            .description("Should the cell values be written to CSV using the formatting applied in Excel, or should they be printed as raw values.")
            .allowableValues("true", "false")
            .defaultValue("false")
            .required(true)
            .build();

    public static final Relationship ORIGINAL = new Relationship.Builder()
            .name("original")
            .description("Original Excel document received by this processor")
            .build();

    public static final Relationship SUCCESS = new Relationship.Builder()
            .name("success")
            .description("Excel data converted to csv")
            .build();

    public static final Relationship FAILURE = new Relationship.Builder()
            .name("failure")
            .description("Failed to parse the Excel document")
            .build();

    private List<PropertyDescriptor> descriptors;

    private Set<Relationship> relationships;

    @Override
    protected void init(final ProcessorInitializationContext context) {
        final List<PropertyDescriptor> descriptors = new ArrayList<>();
        descriptors.add(DESIRED_SHEETS);
        descriptors.add(ROWS_TO_SKIP);
        descriptors.add(COLUMNS_TO_SKIP);
        descriptors.add(FORMAT_VALUES);

        descriptors.add(CSVUtils.CSV_FORMAT);
        descriptors.add(CSVUtils.VALUE_SEPARATOR);
        descriptors.add(CSVUtils.INCLUDE_HEADER_LINE);
        descriptors.add(CSVUtils.QUOTE_CHAR);
        descriptors.add(CSVUtils.ESCAPE_CHAR);
        descriptors.add(CSVUtils.COMMENT_MARKER);
        descriptors.add(CSVUtils.NULL_STRING);
        descriptors.add(CSVUtils.TRIM_FIELDS);
        descriptors.add(new PropertyDescriptor.Builder()
                .fromPropertyDescriptor(CSVUtils.QUOTE_MODE)
                .defaultValue(CSVUtils.QUOTE_NONE.getValue())
                .build());
        descriptors.add(CSVUtils.RECORD_SEPARATOR);
        descriptors.add(CSVUtils.TRAILING_DELIMITER);
        this.descriptors = Collections.unmodifiableList(descriptors);

        final Set<Relationship> relationships = new LinkedHashSet<>();
        relationships.add(ORIGINAL);
        relationships.add(SUCCESS);
        relationships.add(FAILURE);
        this.relationships = Collections.unmodifiableSet(relationships);
    }

    @Override
    public Set<Relationship> getRelationships() {
        return this.relationships;
    }

    @Override
    public final List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return descriptors;
    }

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {
        final FlowFile flowFile = session.get();
        if (flowFile == null) {
            return;
        }

        final Map<String, Boolean> desiredSheets = getDesiredSheets(context, flowFile);
        final boolean formatValues = context.getProperty(FORMAT_VALUES).asBoolean();
        final CSVFormat csvFormat = CSVUtils.createCSVFormat(context, flowFile.getAttributes());

        //Switch to 0 based index
        final int firstRow = context.getProperty(ROWS_TO_SKIP).evaluateAttributeExpressions(flowFile).asInteger() - 1;
        final List<Integer> columnsToSkip = getColumnsToSkip(context, flowFile);

        try {
            session.read(flowFile, inputStream -> {
                try (Workbook workbook = StreamingReader.builder()
                        .rowCacheSize(100)
                        .bufferSize(4096)
                        .setReadStyles(formatValues)
                        .open(inputStream)) {

                    if (!desiredSheets.isEmpty()) {
                        desiredSheets.keySet().forEach(desiredSheet -> workbook.forEach(sheet -> {
                           if (sheet.getSheetName().equalsIgnoreCase(desiredSheet)) {
                               ExcelSheetReadConfig readConfig = new ExcelSheetReadConfig(columnsToSkip, firstRow, sheet.getSheetName());
                               handleExcelSheet(session, flowFile, sheet, readConfig, csvFormat);
                               desiredSheets.put(desiredSheet, Boolean.TRUE);
                           }
                       }));

                        String sheetsNotFound = getSheetsNotFound(desiredSheets);
                        if (!sheetsNotFound.isEmpty()) {
                            getLogger().warn("Excel sheet(s) not found: {}", sheetsNotFound);
                        }
                    } else {
                        workbook.forEach(sheet -> {
                            ExcelSheetReadConfig readConfig = new ExcelSheetReadConfig(columnsToSkip, firstRow, sheet.getSheetName());
                            handleExcelSheet(session, flowFile, sheet, readConfig, csvFormat);
                        });
                    }
                } catch (ParseException | OpenException | ReadException e) {
                    if (e.getCause() instanceof InvalidFormatException) {
                        String msg = "Only .xlsx Excel 2007 OOXML files are supported";
                        getLogger().error(msg, e);
                        throw new UnsupportedOperationException(msg, e);
                    }
                    getLogger().error("Error occurred while processing Excel document metadata", e);
                }
            });

            session.transfer(flowFile, ORIGINAL);

        } catch (RuntimeException ex) {
            getLogger().error("Failed to process incoming Excel document. " + ex.getMessage(), ex);
            FlowFile failedFlowFile = session.putAttribute(flowFile,
                    ConvertExcelToCSVProcessor.class.getName() + ".error", ex.getMessage());
            session.transfer(failedFlowFile, FAILURE);
        }
    }

    private List<Integer> getColumnsToSkip(final ProcessContext context, FlowFile flowFile) {
        final String[] columnsToSkip = StringUtils.split(context.getProperty(COLUMNS_TO_SKIP)
                .evaluateAttributeExpressions(flowFile).getValue(), ",");

        if (columnsToSkip != null) {
            try {
                return Arrays.stream(columnsToSkip)
                        .map(columnToSkip -> Integer.parseInt(columnToSkip) - 1)
                        .collect(Collectors.toList());
            } catch (NumberFormatException e) {
                throw new ProcessException("Invalid column in Columns to Skip list.", e);
            }
        }

        return new ArrayList<>();
    }

    private Map<String, Boolean> getDesiredSheets(final ProcessContext context, FlowFile flowFile) {
        final String desiredSheetsDelimited = context.getProperty(DESIRED_SHEETS).evaluateAttributeExpressions(flowFile).getValue();
        if (desiredSheetsDelimited != null) {
            String[] desiredSheets = StringUtils.split(desiredSheetsDelimited, DESIRED_SHEETS_DELIMITER);
            if (desiredSheets != null) {
                return Arrays.stream(desiredSheets)
                        .collect(Collectors.toMap(key -> key, value -> Boolean.FALSE));
            } else {
                getLogger().debug("Excel document was parsed but no sheets with the specified desired names were found.");
            }
        }

        return new HashMap<>();
    }

    /**
     * Handles an individual Excel sheet from the entire Excel document. Each sheet will result in an individual flowfile.
     *
     * @param session The NiFi ProcessSession instance for the current invocation.
     */
    private void handleExcelSheet(ProcessSession session, FlowFile originalParentFF, final Sheet sheet, ExcelSheetReadConfig readConfig,
                                  CSVFormat csvFormat) {

        FlowFile ff = session.create(originalParentFF);
        final SheetToCSV sheetHandler = new SheetToCSV(readConfig, csvFormat);
        try {
            ff = session.write(ff, out -> {
                PrintStream outPrint = new PrintStream(out, false, StandardCharsets.UTF_8);
                sheetHandler.setOutput(outPrint);
                sheet.forEach(row -> {
                    sheetHandler.startRow(row.getRowNum());
                    row.forEach(sheetHandler::cell);
                    sheetHandler.endRow();
                });
                sheetHandler.close();
            });

            ff = session.putAttribute(ff, SHEET_NAME, readConfig.getSheetName());
            ff = session.putAttribute(ff, ROW_NUM, Long.toString(sheetHandler.getRowCount()));

            if (StringUtils.isNotEmpty(originalParentFF.getAttribute(CoreAttributes.FILENAME.key()))) {
                ff = session.putAttribute(ff, SOURCE_FILE_NAME, originalParentFF.getAttribute(CoreAttributes.FILENAME.key()));
            } else {
                ff = session.putAttribute(ff, SOURCE_FILE_NAME, UNKNOWN_SHEET_NAME);
            }

            //Update the CoreAttributes.FILENAME to have the .csv extension now. Also update MIME.TYPE
            ff = session.putAttribute(ff, CoreAttributes.FILENAME.key(), updateFilenameToCSVExtension(ff.getAttribute(CoreAttributes.UUID.key()),
                    ff.getAttribute(CoreAttributes.FILENAME.key()), readConfig.getSheetName()));
            ff = session.putAttribute(ff, CoreAttributes.MIME_TYPE.key(), CSV_MIME_TYPE);

            session.transfer(ff, SUCCESS);

        } catch (RuntimeException e) {
            ff = session.putAttribute(ff, ConvertExcelToCSVProcessor.class.getName() + ".error", e.getMessage());
            session.transfer(ff, FAILURE);
        }
    }

    private String getSheetsNotFound(Map<String, Boolean> desiredSheets) {
        return desiredSheets.entrySet().stream()
                .filter(entry -> !entry.getValue())
                .map(Map.Entry::getKey)
                .collect(Collectors.joining(","));
    }

    /**
     * Uses the com.github.pjfanning streaming cell implementation to
     * do most of the work of parsing the contents of the Excel sheet
     * and outputs the contents as a (basic) CSV.
     */
    private class SheetToCSV {
        private final ExcelSheetReadConfig readConfig;
        CSVFormat csvFormat;
        private boolean firstCellOfRow;
        private boolean skipRow;
        private int currentRow = -1;
        private int currentCol = -1;
        private int rowCount = 0;
        private int skippedColumns = 0;
        private CSVPrinter printer;
        private boolean firstRow = false;
        private ArrayList<String> fieldValues;

        public int getRowCount() {
            return rowCount;
        }

        public void setOutput(PrintStream output) {
            final OutputStreamWriter streamWriter = new OutputStreamWriter(output, StandardCharsets.UTF_8);

            try {
                printer = new CSVPrinter(streamWriter, csvFormat);
            } catch (IOException e) {
                throw new ProcessException("Failed to create CSV Printer.", e);
            }
        }

        public SheetToCSV(ExcelSheetReadConfig readConfig, CSVFormat csvFormat) {
            this.readConfig = readConfig;
            this.csvFormat = csvFormat;
        }

        public void startRow(int rowNum) {
            if (rowNum <= readConfig.getOverrideFirstRow()) {
                skipRow = true;
                return;
            }

            // Prepare for this row
            skipRow = false;
            firstCellOfRow = true;
            firstRow = currentRow == -1;
            currentRow = rowNum;
            currentCol = -1;
            fieldValues = new ArrayList<>();
        }

        public void endRow() {
            if(skipRow) {
                return;
            }

            if(firstRow) {
                readConfig.setLastColumn(currentCol);
            }

            //if there was no data in this row, don't write it
            if(fieldValues.stream()
                    .noneMatch(string -> string != null && !string.isEmpty())) {
                return;
            }

            // Ensure the correct number of columns
            int columnsToAdd = (readConfig.getLastColumn() - currentCol) - readConfig.getColumnsToSkip().size();
            for (int i = 0; i < columnsToAdd; i++) {
                fieldValues.add(null);
            }

            try {
                printer.printRecord(fieldValues);
            } catch (IOException e) {
                getLogger().warn("Print Record failed", e);
            }

            rowCount++;
        }

        public void cell(Cell cell) {
            if (skipRow) {
                return;
            }

            // Did we miss any cells?
            int thisCol = cell.getColumnIndex();

            //Use the first row of the file to decide on the area of data to export
            if (firstRow && firstCellOfRow) {
                readConfig.setFirstColumn(thisCol);
            }

            //if this cell falls outside our area, or has been explicitly marked as a skipped column, return and don't write it out.
            if (!firstRow && (thisCol < readConfig.getFirstColumn() || thisCol > readConfig.getLastColumn())) {
                return;
            }

            if (readConfig.getColumnsToSkip().contains(thisCol)) {
                skippedColumns++;
                return;
            }

            int missedCols = (thisCol - readConfig.getFirstColumn()) - (currentCol - readConfig.getFirstColumn()) - 1;
            if (firstCellOfRow) {
                missedCols = (thisCol - readConfig.getFirstColumn());
            }

            missedCols -= skippedColumns;

            if (firstCellOfRow) {
                firstCellOfRow = false;
            }

            for (int i = 0; i < missedCols; i++) {
                fieldValues.add(null);
            }
            currentCol = thisCol;

            String stringCellValue = cell.getStringCellValue();
            fieldValues.add(stringCellValue != null && !stringCellValue.isEmpty() ? stringCellValue : null);

            skippedColumns = 0;
        }

        public void close() throws IOException {
            printer.close();
        }
    }

    /**
     * Takes the original input filename and updates it by removing the file extension and replacing it with
     * the .csv extension.
     *
     * @param origFileName Original filename from the input file.
     * @return The new filename with the .csv extension that should be place in the output flowfile's attributes
     */
    private String updateFilenameToCSVExtension(String nifiUUID, String origFileName, String sheetName) {

        StringBuilder stringBuilder = new StringBuilder();

        if (StringUtils.isNotEmpty(origFileName)) {
            String ext = FilenameUtils.getExtension(origFileName);
            if (StringUtils.isNotEmpty(ext)) {
                stringBuilder.append(StringUtils.replace(origFileName, ("." + ext), ""));
            } else {
                stringBuilder.append(origFileName);
            }
        } else {
            stringBuilder.append(nifiUUID);
        }

        stringBuilder.append("_");
        stringBuilder.append(sheetName);
        stringBuilder.append(".");
        stringBuilder.append("csv");

        return stringBuilder.toString();
    }

    private static class ExcelSheetReadConfig {
        public String getSheetName() {
            return sheetName;
        }

        public int getFirstColumn() {
            return firstColumn;
        }

        public void setFirstColumn(int value) {
            this.firstColumn = value;
        }

        public int getLastColumn() {
            return lastColumn;
        }

        public void setLastColumn(int lastColumn) {
            this.lastColumn = lastColumn;
        }

        public int getOverrideFirstRow() {
            return overrideFirstRow;
        }

        public List<Integer> getColumnsToSkip() {
            return columnsToSkip;
        }

        private int firstColumn;
        private int lastColumn;
        private final int overrideFirstRow;
        private final String sheetName;
        private final List<Integer> columnsToSkip;

        public ExcelSheetReadConfig(List<Integer> columnsToSkip, int overrideFirstRow, String sheetName) {

            this.sheetName = sheetName;
            this.columnsToSkip = columnsToSkip;
            this.overrideFirstRow = overrideFirstRow;
        }
    }
}
