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
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.io.FilenameUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.behavior.WritesAttributes;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.components.PropertyDescriptor;
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
import org.apache.poi.xssf.eventusermodel.XSSFReader;
import org.apache.poi.xssf.model.SharedStringsTable;
import org.apache.poi.xssf.usermodel.XSSFRichTextString;
import org.xml.sax.Attributes;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;
import org.xml.sax.XMLReader;
import org.xml.sax.helpers.DefaultHandler;
import org.xml.sax.helpers.XMLReaderFactory;


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
    private static final String SAX_CELL_REF = "c";
    private static final String SAX_CELL_TYPE = "t";
    private static final String SAX_CELL_ADDRESS = "r";
    private static final String SAX_CELL_STRING = "s";
    private static final String SAX_CELL_CONTENT_REF = "v";
    private static final String SAX_ROW_REF = "row";
    private static final String SAX_SHEET_NAME_REF = "sheetPr";
    private static final String DESIRED_SHEETS_DELIMITER = ",";
    private static final String UNKNOWN_SHEET_NAME = "UNKNOWN";
    private static final String SAX_PARSER = "org.apache.xerces.parsers.SAXParser";
    private static final Pattern CELL_ADDRESS_REGEX = Pattern.compile("^([a-zA-Z]+)([\\d]+)$");

    public static final PropertyDescriptor DESIRED_SHEETS = new PropertyDescriptor
            .Builder().name("extract-sheets")
            .displayName("Sheets to Extract")
            .description("Comma separated list of Excel document sheet names that should be extracted from the excel document. If this property" +
                    " is left blank then all of the sheets will be extracted from the Excel document. The list of names is case in-sensitive. Any sheets not " +
                    "specified in this value will be ignored.")
            .required(false)
            .expressionLanguageSupported(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
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

        try {

            session.read(flowFile, new InputStreamCallback() {
                @Override
                public void process(InputStream inputStream) throws IOException {

                    try {
                        String desiredSheetsDelimited = context.getProperty(DESIRED_SHEETS)
                                .evaluateAttributeExpressions().getValue();

                        OPCPackage pkg = OPCPackage.open(inputStream);
                        XSSFReader r = new XSSFReader(pkg);
                        SharedStringsTable sst = r.getSharedStringsTable();
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
                                            handleExcelSheet(session, flowFile, sst, sheet, sheetName);
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
                                handleExcelSheet(session, flowFile, sst, iter.next(), iter.getSheetName());
                            }
                        }
                    } catch (InvalidFormatException ife) {
                        getLogger().error("Only .xlsx Excel 2007 OOXML files are supported", ife);
                        throw new UnsupportedOperationException("Only .xlsx Excel 2007 OOXML files are supported", ife);
                    } catch (OpenXML4JException e) {
                        getLogger().error("Error occurred while processing Excel document metadata", e);
                    }
                }
            });

            session.transfer(flowFile, ORIGINAL);

        } catch (RuntimeException ex) {
            getLogger().error("Failed to process incoming Excel document", ex);
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
    private void handleExcelSheet(ProcessSession session, FlowFile originalParentFF,
            SharedStringsTable sst, final InputStream sheetInputStream, String sName) throws IOException {

        FlowFile ff = session.create();
        try {

            XMLReader parser =
                    XMLReaderFactory.createXMLReader(
                            SAX_PARSER
                    );
            ExcelSheetRowHandler handler = new ExcelSheetRowHandler(sst);
            parser.setContentHandler(handler);

            ff = session.write(ff, new OutputStreamCallback() {
                @Override
                public void process(OutputStream out) throws IOException {
                    InputSource sheetSource = new InputSource(sheetInputStream);
                    ExcelSheetRowHandler eh = null;
                    try {
                        eh = (ExcelSheetRowHandler) parser.getContentHandler();
                        eh.setFlowFileOutputStream(out);
                        parser.setContentHandler(eh);
                        parser.parse(sheetSource);
                        sheetInputStream.close();
                    } catch (SAXException se) {
                        getLogger().error("Error occurred while processing Excel sheet {}", new Object[]{eh.getSheetName()}, se);
                    }
                }
            });

            if (handler.getSheetName().equals(UNKNOWN_SHEET_NAME)) {
                //Used the named parsed from the handler. This logic is only here because IF the handler does find a value that should take precedence.
                ff = session.putAttribute(ff, SHEET_NAME, sName);
            } else {
                ff = session.putAttribute(ff, SHEET_NAME, handler.getSheetName());
                sName = handler.getSheetName();
            }

            ff = session.putAttribute(ff, ROW_NUM, new Long(handler.getRowCount()).toString());

            if (StringUtils.isNotEmpty(originalParentFF.getAttribute(CoreAttributes.FILENAME.key()))) {
                ff = session.putAttribute(ff, SOURCE_FILE_NAME, originalParentFF.getAttribute(CoreAttributes.FILENAME.key()));
            } else {
                ff = session.putAttribute(ff, SOURCE_FILE_NAME, UNKNOWN_SHEET_NAME);
            }

            //Update the CoreAttributes.FILENAME to have the .csv extension now. Also update MIME.TYPE
            ff = session.putAttribute(ff, CoreAttributes.FILENAME.key(), updateFilenameToCSVExtension(ff.getAttribute(CoreAttributes.UUID.key()),
                    ff.getAttribute(CoreAttributes.FILENAME.key()), sName));
            ff = session.putAttribute(ff, CoreAttributes.MIME_TYPE.key(), CSV_MIME_TYPE);

            session.transfer(ff, SUCCESS);

        } catch (SAXException saxE) {
            getLogger().error("Failed to create instance of SAXParser {}", new Object[]{SAX_PARSER}, saxE);
            ff = session.putAttribute(ff,
                    ConvertExcelToCSVProcessor.class.getName() + ".error", saxE.getMessage());
            session.transfer(ff, FAILURE);
        } finally {
            sheetInputStream.close();
        }
    }

    static Integer columnToIndex(String col) {
        int length = col.length();
        int accumulator = 0;
        for (int i = length; i > 0; i--) {
            char c = col.charAt(i - 1);
            int x = ((int) c) - 64;
            accumulator += x * Math.pow(26, length - i);
        }
        // Make it to start with 0.
        return accumulator - 1;
    }

    private static class CellAddress {
        final int row;
        final int col;

        private CellAddress(int row, int col) {
            this.row = row;
            this.col = col;
        }
    }

    /**
     * Extracts every row from an Excel Sheet and generates a corresponding JSONObject whose key is the Excel CellAddress and value
     * is the content of that CellAddress converted to a String
     */
    private class ExcelSheetRowHandler
            extends DefaultHandler {

        private SharedStringsTable sst;
        private String currentContent;
        private boolean nextIsString;
        private CellAddress firstCellAddress;
        private CellAddress firstRowLastCellAddress;
        private CellAddress previousCellAddress;
        private CellAddress nextCellAddress;
        private OutputStream outputStream;
        private boolean firstColInRow;
        long rowCount;
        String sheetName;

        private ExcelSheetRowHandler(SharedStringsTable sst) {
            this.sst = sst;
            this.firstColInRow = true;
            this.rowCount = 0l;
            this.sheetName = UNKNOWN_SHEET_NAME;
        }

        public void setFlowFileOutputStream(OutputStream outputStream) {
            this.outputStream = outputStream;
        }


        public void startElement(String uri, String localName, String name,
                Attributes attributes) throws SAXException {

            if (name.equals(SAX_CELL_REF)) {
                String cellType = attributes.getValue(SAX_CELL_TYPE);
                // Analyze cell address.
                Matcher cellAddressMatcher = CELL_ADDRESS_REGEX.matcher(attributes.getValue(SAX_CELL_ADDRESS));
                if (cellAddressMatcher.matches()) {
                    String col = cellAddressMatcher.group(1);
                    String row = cellAddressMatcher.group(2);
                    nextCellAddress = new CellAddress(Integer.parseInt(row), columnToIndex(col));

                    if (firstCellAddress == null) {
                        firstCellAddress = nextCellAddress;
                    }
                }
                if (cellType != null && cellType.equals(SAX_CELL_STRING)) {
                    nextIsString = true;
                } else {
                    nextIsString = false;
                }
            } else if (name.equals(SAX_ROW_REF)) {
                if (firstRowLastCellAddress == null) {
                    firstRowLastCellAddress = previousCellAddress;
                }
                firstColInRow = true;
                previousCellAddress = null;
                nextCellAddress = null;
            } else if (name.equals(SAX_SHEET_NAME_REF)) {
                sheetName = attributes.getValue(0);
            }

            currentContent = "";
        }

        private void fillEmptyColumns(int nextColumn) throws IOException {
            final CellAddress previousCell = previousCellAddress != null ? previousCellAddress : firstCellAddress;
            if (previousCell != null) {
                for (int i = 0; i < (nextColumn - previousCell.col); i++) {
                    // Fill columns.
                    outputStream.write(",".getBytes());
                }
            }
        }

        public void endElement(String uri, String localName, String name)
                throws SAXException {

            if (nextIsString) {
                int idx = Integer.parseInt(currentContent);
                currentContent = new XSSFRichTextString(sst.getEntryAt(idx)).toString();
                nextIsString = false;
            }

            if (name.equals(SAX_CELL_CONTENT_REF)
                    // Limit scanning from the first column, and up to the last column.
                    && (firstCellAddress == null || firstCellAddress.col <= nextCellAddress.col)
                    && (firstRowLastCellAddress == null || nextCellAddress.col <= firstRowLastCellAddress.col)) {
                try {
                    // A cell is found.
                    fillEmptyColumns(nextCellAddress.col);
                    firstColInRow = false;
                    outputStream.write(currentContent.getBytes());
                    // Keep previously found cell address.
                    previousCellAddress = nextCellAddress;
                } catch (IOException e) {
                    getLogger().error("IO error encountered while writing content of parsed cell " +
                            "value from sheet {}", new Object[]{getSheetName()}, e);
                }
            }

            if (name.equals(SAX_ROW_REF)) {
                //If this is the first row and the end of the row element has been encountered then that means no columns were present.
                if (!firstColInRow) {
                    try {
                        if (firstRowLastCellAddress != null) {
                            fillEmptyColumns(firstRowLastCellAddress.col);
                        }
                        rowCount++;
                        outputStream.write("\n".getBytes());
                    } catch (IOException e) {
                        getLogger().error("IO error encountered while writing new line indicator", e);
                    }
                }
            }

        }

        public void characters(char[] ch, int start, int length)
                throws SAXException {
            currentContent += new String(ch, start, length);
        }

        public long getRowCount() {
            return rowCount;
        }

        public String getSheetName() {
            return sheetName;
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

}