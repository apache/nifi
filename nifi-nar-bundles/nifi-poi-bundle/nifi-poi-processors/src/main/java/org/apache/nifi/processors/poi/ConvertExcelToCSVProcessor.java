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

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

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
import org.apache.nifi.processor.io.InputStreamCallback;
import org.apache.nifi.processor.io.OutputStreamCallback;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.poi.openxml4j.exceptions.InvalidFormatException;
import org.apache.poi.openxml4j.exceptions.OpenXML4JException;
import org.apache.poi.openxml4j.opc.OPCPackage;
import org.apache.poi.ss.usermodel.DataFormatter;
import org.apache.poi.ss.util.CellAddress;
import org.apache.poi.ss.util.CellReference;
import org.apache.poi.ooxml.util.SAXHelper;
import org.apache.poi.xssf.eventusermodel.ReadOnlySharedStringsTable;
import org.apache.poi.xssf.eventusermodel.XSSFReader;
import org.apache.poi.xssf.eventusermodel.XSSFSheetXMLHandler;
import org.apache.poi.xssf.model.StylesTable;
import org.apache.poi.xssf.usermodel.XSSFComment;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;
import org.xml.sax.XMLReader;

import javax.xml.parsers.ParserConfigurationException;


@Tags({"excel", "csv", "poi"})
@CapabilityDescription("Consumes a Microsoft Excel document and converts each worksheet to csv. Each sheet from the incoming Excel " +
        "document will generate a new Flowfile that will be output from this processor. Each output Flowfile's contents will be formatted as a csv file " +
        "where the each row from the excel sheet is output as a newline in the csv file. This processor is currently only capable of processing .xlsx " +
        "(XSSF 2007 OOXML file format) Excel documents and not older .xls (HSSF '97(-2007) file format) documents. This processor also expects well formatted " +
        "CSV content and will not escape cell's containing invalid content such as newlines or additional commas.")
@WritesAttributes({@WritesAttribute(attribute="sheetname", description="The name of the Excel sheet that this particular row of data came from in the Excel document"),
        @WritesAttribute(attribute="numrows", description="The number of rows in this Excel Sheet"),
        @WritesAttribute(attribute="sourcefilename", description="The name of the Excel document file that this data originated from"),
        @WritesAttribute(attribute="convertexceltocsvprocessor.error", description="Error message that was encountered on a per Excel sheet basis. This attribute is" +
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
                    "specified in this value will be ignored.")
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

        final Set<Relationship> relationships = new HashSet<>();
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
        if ( flowFile == null ) {
            return;
        }

        final String desiredSheetsDelimited = context.getProperty(DESIRED_SHEETS).evaluateAttributeExpressions(flowFile).getValue();
        final boolean formatValues = context.getProperty(FORMAT_VALUES).asBoolean();

        final CSVFormat csvFormat = CSVUtils.createCSVFormat(context);

        //Switch to 0 based index
        final int firstRow = context.getProperty(ROWS_TO_SKIP).evaluateAttributeExpressions(flowFile).asInteger() - 1;
        final String[] sColumnsToSkip = StringUtils
                .split(context.getProperty(COLUMNS_TO_SKIP).evaluateAttributeExpressions(flowFile).getValue(), ",");

        final List<Integer> columnsToSkip = new ArrayList<>();

        if(sColumnsToSkip != null && sColumnsToSkip.length > 0) {
            for (String c : sColumnsToSkip) {
                try {
                    //Switch to 0 based index
                    columnsToSkip.add(Integer.parseInt(c) - 1);
                } catch (NumberFormatException e) {
                    throw new ProcessException("Invalid column in Columns to Skip list.", e);
                }
            }
        }

        try {
            session.read(flowFile, new InputStreamCallback() {
                @Override
                public void process(InputStream inputStream) throws IOException {

                    try {
                        OPCPackage pkg = OPCPackage.open(inputStream);
                        XSSFReader r = new XSSFReader(pkg);
                        ReadOnlySharedStringsTable sst = new ReadOnlySharedStringsTable(pkg);
                        StylesTable styles = r.getStylesTable();
                        XSSFReader.SheetIterator iter = (XSSFReader.SheetIterator) r.getSheetsData();

                        if (desiredSheetsDelimited != null) {
                            String[] desiredSheets = StringUtils
                                    .split(desiredSheetsDelimited, DESIRED_SHEETS_DELIMITER);

                            if (desiredSheets != null) {
                                while (iter.hasNext()) {
                                    InputStream sheet = iter.next();
                                    String sheetName = iter.getSheetName();

                                    for (int i = 0; i < desiredSheets.length; i++) {
                                        //If the sheetName is a desired one parse it
                                        if (sheetName.equalsIgnoreCase(desiredSheets[i])) {
                                            ExcelSheetReadConfig readConfig = new ExcelSheetReadConfig(columnsToSkip, firstRow, sheetName, formatValues, sst, styles);
                                            handleExcelSheet(session, flowFile, sheet, readConfig, csvFormat);
                                            break;
                                        }
                                    }
                                }
                            } else {
                                getLogger().debug("Excel document was parsed but no sheets with the specified desired names were found.");
                            }

                        } else {
                            //Get all of the sheets in the document.
                            while (iter.hasNext()) {
                                InputStream sheet = iter.next();
                                String sheetName = iter.getSheetName();

                                ExcelSheetReadConfig readConfig = new ExcelSheetReadConfig(columnsToSkip, firstRow, sheetName, formatValues, sst, styles);
                                handleExcelSheet(session, flowFile, sheet, readConfig, csvFormat);
                            }
                        }
                    } catch (InvalidFormatException ife) {
                        getLogger().error("Only .xlsx Excel 2007 OOXML files are supported", ife);
                        throw new UnsupportedOperationException("Only .xlsx Excel 2007 OOXML files are supported", ife);
                    } catch (OpenXML4JException | SAXException e) {
                        getLogger().error("Error occurred while processing Excel document metadata", e);
                    }
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


    /**
     * Handles an individual Excel sheet from the entire Excel document. Each sheet will result in an individual flowfile.
     *
     * @param session
     *  The NiFi ProcessSession instance for the current invocation.
     */
    private void handleExcelSheet(ProcessSession session, FlowFile originalParentFF, final InputStream sheetInputStream, ExcelSheetReadConfig readConfig,
                                  CSVFormat csvFormat) throws IOException {

        FlowFile ff = session.create(originalParentFF);
        try {
            final DataFormatter formatter = new DataFormatter();
            final InputSource sheetSource = new InputSource(sheetInputStream);

            final SheetToCSV sheetHandler = new SheetToCSV(readConfig, csvFormat);

            final XMLReader parser = SAXHelper.newXMLReader();

            //If Value Formatting is set to false then don't pass in the styles table.
            // This will cause the XSSF Handler to return the raw value instead of the formatted one.
            final StylesTable sst = readConfig.getFormatValues()?readConfig.getStyles():null;

            final XSSFSheetXMLHandler handler = new XSSFSheetXMLHandler(
                    sst, null, readConfig.getSharedStringsTable(), sheetHandler, formatter, false);

            parser.setContentHandler(handler);

            ff = session.write(ff, new OutputStreamCallback() {
                @Override
                public void process(OutputStream out) throws IOException {
                    PrintStream outPrint = new PrintStream(out);
                    sheetHandler.setOutput(outPrint);

                    try {
                        parser.parse(sheetSource);

                        sheetInputStream.close();

                        sheetHandler.close();
                        outPrint.close();
                    } catch (SAXException se) {
                        getLogger().error("Error occurred while processing Excel sheet {}", new Object[]{readConfig.getSheetName()}, se);
                    }
                }
            });

            ff = session.putAttribute(ff, SHEET_NAME, readConfig.getSheetName());
            ff = session.putAttribute(ff, ROW_NUM, new Long(sheetHandler.getRowCount()).toString());

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

        } catch (SAXException | ParserConfigurationException saxE) {
            getLogger().error("Failed to create instance of Parser.", saxE);
            ff = session.putAttribute(ff,
                    ConvertExcelToCSVProcessor.class.getName() + ".error", saxE.getMessage());
            session.transfer(ff, FAILURE);
        } finally {
            sheetInputStream.close();
        }
    }

    /**
     * Uses the XSSF Event SAX helpers to do most of the work
     *  of parsing the Sheet XML, and outputs the contents
     *  as a (basic) CSV.
     */
    private class SheetToCSV implements XSSFSheetXMLHandler.SheetContentsHandler {
        private ExcelSheetReadConfig readConfig;
        CSVFormat csvFormat;

        private boolean firstCellOfRow;
        private boolean skipRow;
        private int currentRow = -1;
        private int currentCol = -1;
        private int rowCount = 0;
        private boolean rowHasValues=false;
        private int skippedColumns=0;

        private CSVPrinter printer;

        private boolean firstRow=false;

        private ArrayList<Object> fieldValues;

        public int getRowCount(){
            return rowCount;
        }

        public void setOutput(PrintStream output){
            final OutputStreamWriter streamWriter = new OutputStreamWriter(output);

            try {
                printer = new CSVPrinter(streamWriter, csvFormat);
            } catch (IOException e) {
                throw new ProcessException("Failed to create CSV Printer.", e);
            }
        }

        public SheetToCSV(ExcelSheetReadConfig readConfig, CSVFormat csvFormat){
            this.readConfig = readConfig;
            this.csvFormat = csvFormat;
        }

        @Override
        public void startRow(int rowNum) {
            if(rowNum <= readConfig.getOverrideFirstRow()) {
                skipRow = true;
                return;
            }

            // Prepare for this row
            skipRow = false;
            firstCellOfRow = true;
            firstRow = currentRow==-1;
            currentRow = rowNum;
            currentCol = -1;
            rowHasValues = false;

            fieldValues = new ArrayList<>();
        }

        @Override
        public void endRow(int rowNum) {
            if(skipRow) {
                return;
            }

            if(firstRow){
                readConfig.setLastColumn(currentCol);
            }

            //if there was no data in this row, don't write it
            if(!rowHasValues) {
                return;
            }

            // Ensure the correct number of columns
            int columnsToAdd = (readConfig.getLastColumn() - currentCol) - readConfig.getColumnsToSkip().size();
            for (int i=0; i<columnsToAdd; i++) {
                fieldValues.add(null);
            }

            try {
                printer.printRecord(fieldValues);
            } catch (IOException e) {
                e.printStackTrace();
            }

            rowCount++;
        }

        @Override
        public void cell(String cellReference, String formattedValue,
                         XSSFComment comment) {
            if(skipRow) {
                return;
            }

            // gracefully handle missing CellRef here in a similar way as XSSFCell does
            if(cellReference == null) {
                cellReference = new CellAddress(currentRow, currentCol).formatAsString();
            }

            // Did we miss any cells?
            int thisCol = (new CellReference(cellReference)).getCol();

            // Should we skip this

            //Use the first row of the file to decide on the area of data to export
            if(firstRow && firstCellOfRow){
                readConfig.setFirstRow(currentRow);
                readConfig.setFirstColumn(thisCol);
            }

            //if this cell falls outside our area, or has been explcitely marked as a skipped column, return and don't write it out.
            if(!firstRow && (thisCol < readConfig.getFirstColumn() || thisCol > readConfig.getLastColumn())){
                return;
            }

            if(readConfig.getColumnsToSkip().contains(thisCol)){
                skippedColumns++;
                return;
            }

            int missedCols = (thisCol - readConfig.getFirstColumn()) - (currentCol - readConfig.getFirstColumn()) - 1;
            if(firstCellOfRow){
                missedCols = (thisCol - readConfig.getFirstColumn());
            }

            missedCols -= skippedColumns;

            if (firstCellOfRow) {
                firstCellOfRow = false;
            }

            for (int i=0; i<missedCols; i++) {
                fieldValues.add(null);
            }
            currentCol = thisCol;

            fieldValues.add(formattedValue);

            rowHasValues = true;
            skippedColumns = 0;
        }

        @Override
        public void headerFooter(String s, boolean b, String s1) {

        }

        public void close() throws IOException {
            printer.close();
        }
    }

    /**
     * Takes the original input filename and updates it by removing the file extension and replacing it with
     * the .csv extension.
     *
     * @param origFileName
     *  Original filename from the input file.
     *
     * @return
     *  The new filename with the .csv extension that should be place in the output flowfile's attributes
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

    private class ExcelSheetReadConfig {
        public String getSheetName(){
            return sheetName;
        }

        public int getFirstColumn(){
            return firstColumn;
        }

        public void setFirstColumn(int value){
            this.firstColumn = value;
        }

        public int getLastColumn(){
            return lastColumn;
        }

        public void setLastColumn(int lastColumn) {
            this.lastColumn = lastColumn;
        }

        public int getOverrideFirstRow(){
            return overrideFirstRow;
        }

        public boolean getFormatValues() {
            return formatValues;
        }

        public int getFirstRow(){
            return firstRow;
        }

        public void setFirstRow(int value){
            firstRow = value;
        }

        public int getLastRow(){
            return lastRow;
        }

        public void setLastRow(int value){
            lastRow = value;
        }

        public List<Integer> getColumnsToSkip(){
            return columnsToSkip;
        }

        public ReadOnlySharedStringsTable getSharedStringsTable(){
            return sst;
        }

        public StylesTable getStyles(){
            return styles;
        }

        private int firstColumn;
        private int lastColumn;

        private int firstRow;
        private int lastRow;
        private int overrideFirstRow;
        private String sheetName;
        private boolean formatValues;

        private ReadOnlySharedStringsTable sst;
        private StylesTable styles;

        private List<Integer> columnsToSkip;

        public ExcelSheetReadConfig(List<Integer> columnsToSkip, int overrideFirstRow, String sheetName, boolean formatValues,
                                    ReadOnlySharedStringsTable sst, StylesTable styles){

            this.sheetName = sheetName;
            this.columnsToSkip = columnsToSkip;
            this.overrideFirstRow = overrideFirstRow;
            this.formatValues = formatValues;

            this.sst = sst;
            this.styles = styles;
        }
    }
}
