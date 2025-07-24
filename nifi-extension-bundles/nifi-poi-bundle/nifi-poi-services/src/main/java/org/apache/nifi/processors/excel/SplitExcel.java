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
package org.apache.nifi.processors.excel;

import com.github.pjfanning.xlsx.StreamingReader;
import com.github.pjfanning.xlsx.exceptions.ExcelRuntimeException;
import com.github.pjfanning.xlsx.impl.XlsxHyperlink;
import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.SideEffectFree;
import org.apache.nifi.annotation.behavior.SupportsBatching;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.behavior.WritesAttributes;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.excel.ProtectionType;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.flowfile.attributes.CoreAttributes;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.poi.ss.usermodel.Cell;
import org.apache.poi.ss.usermodel.CellCopyContext;
import org.apache.poi.ss.usermodel.CellCopyPolicy;
import org.apache.poi.ss.usermodel.Hyperlink;
import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.ss.usermodel.Sheet;
import org.apache.poi.ss.usermodel.Workbook;
import org.apache.poi.ss.util.CellRangeAddress;
import org.apache.poi.ss.util.CellUtil;
import org.apache.poi.xssf.streaming.SXSSFSheet;
import org.apache.poi.xssf.streaming.SXSSFWorkbook;

import java.io.OutputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import static org.apache.nifi.flowfile.attributes.FragmentAttributes.FRAGMENT_COUNT;
import static org.apache.nifi.flowfile.attributes.FragmentAttributes.FRAGMENT_ID;
import static org.apache.nifi.flowfile.attributes.FragmentAttributes.FRAGMENT_INDEX;
import static org.apache.nifi.flowfile.attributes.FragmentAttributes.SEGMENT_ORIGINAL_FILENAME;

@SideEffectFree
@SupportsBatching
@Tags({"split", "text"})
@InputRequirement(InputRequirement.Requirement.INPUT_REQUIRED)
@CapabilityDescription("This processor splits a multi sheet Microsoft Excel spreadsheet into multiple Microsoft Excel spreadsheets where each sheet from the original" +
        " file is converted to an individual spreadsheet in its own flow file. Currently this processor is only capable of processing .xlsx" +
        " (XSSF 2007 OOXML file format) Excel documents and not older .xls (HSSF '97(-2007) file format) documents." +
        " Please note all original cell styles are dropped and formulas are removed leaving only the calculated values." +
        " Even a single sheet Microsoft Excel spreadsheet is converted to its own flow file with all the original cell styles dropped and formulas removed."
)
@WritesAttributes({
        @WritesAttribute(attribute = "fragment.identifier", description = "All split Excel FlowFiles produced from the same parent Excel FlowFile will have the same randomly generated UUID added" +
                " for this attribute"),
        @WritesAttribute(attribute = "fragment.index", description = "A one-up number that indicates the ordering of the split Excel FlowFiles that were created from a single parent Excel FlowFile"),
        @WritesAttribute(attribute = "fragment.count", description = "The number of split Excel FlowFiles generated from the parent Excel FlowFile"),
        @WritesAttribute(attribute = "segment.original.filename", description = "The filename of the parent Excel FlowFile"),
        @WritesAttribute(attribute = SplitExcel.SHEET_NAME, description = "The name of the Excel sheet from the original spreadsheet."),
        @WritesAttribute(attribute = SplitExcel.TOTAL_ROWS, description = "The number of rows in the Excel sheet from the original spreadsheet.")})
public class SplitExcel extends AbstractProcessor {
    public static final String SHEET_NAME = "sheetname";
    public static final String TOTAL_ROWS = "total.rows";

    public static final PropertyDescriptor PROTECTION_TYPE = new PropertyDescriptor.Builder()
            .name("Protection Type")
            .description("Specifies whether an Excel spreadsheet is protected by a password or not.")
            .required(true)
            .allowableValues(ProtectionType.class)
            .defaultValue(ProtectionType.UNPROTECTED)
            .build();

    public static final PropertyDescriptor PASSWORD = new PropertyDescriptor.Builder()
            .name("Password")
            .description("The password for a password protected Excel spreadsheet")
            .required(true)
            .sensitive(true)
            .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
            .dependsOn(PROTECTION_TYPE, ProtectionType.PASSWORD)
            .build();

    public static final Relationship REL_ORIGINAL = new Relationship.Builder()
            .name("original")
            .description("The original FlowFile that was split into segments. If the FlowFile fails processing, nothing will be sent to this relationship")
            .build();

    public static final Relationship REL_FAILURE = new Relationship.Builder()
            .name("failure")
            .description("If a FlowFile cannot be transformed from the configured input format to the configured output format, the unchanged FlowFile will be routed to this relationship.")
            .build();

    public static final Relationship REL_SPLIT = new Relationship.Builder()
            .name("split")
            .description("The individual Excel 'segments' of the original Excel FlowFile will be routed to this relationship.")
            .build();

    private static final List<PropertyDescriptor> PROPERTY_DESCRIPTORS = List.of(
            PROTECTION_TYPE,
            PASSWORD
    );

    private static final Set<Relationship> RELATIONSHIPS = Set.of(
            REL_ORIGINAL,
            REL_FAILURE,
            REL_SPLIT
    );

    private static final CellCopyPolicy CELL_COPY_POLICY = new CellCopyPolicy.Builder()
            .cellFormula(false) // NOTE: setting to false allows for copying the evaluated formula value.
            .cellStyle(CellCopyPolicy.DEFAULT_COPY_CELL_STYLE_POLICY)
            .cellValue(CellCopyPolicy.DEFAULT_COPY_CELL_VALUE_POLICY)
            .condenseRows(CellCopyPolicy.DEFAULT_CONDENSE_ROWS_POLICY)
            .copyHyperlink(false) // NOTE: the hyperlinks appear at end of sheet, so we need to iterate them separately at the end.
            .mergeHyperlink(CellCopyPolicy.DEFAULT_MERGE_HYPERLINK_POLICY)
            .mergedRegions(false) // NOTE: set to false because of the explicit merge region handling in the copyRows method.
            .rowHeight(CellCopyPolicy.DEFAULT_COPY_ROW_HEIGHT_POLICY)
            .build();

    @Override
    public Set<Relationship> getRelationships() {
        return RELATIONSHIPS;
    }

    @Override
    public final List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return PROPERTY_DESCRIPTORS;
    }

    @Override
    public void onTrigger(ProcessContext context, ProcessSession session) throws ProcessException {
        FlowFile originalFlowFile = session.get();
        if (originalFlowFile == null) {
            return;
        }

        final ProtectionType protectionType = context.getProperty(PROTECTION_TYPE).asAllowableValue(ProtectionType.class);
        final String password = protectionType == ProtectionType.PASSWORD ? context.getProperty(PASSWORD).getValue() : null;
        final List<WorkbookSplit> workbookSplits = new ArrayList<>();

        try {
            session.read(originalFlowFile, in -> {

                final Workbook originalWorkbook = StreamingReader.builder()
                        .rowCacheSize(100)
                        .bufferSize(4096)
                        .password(password)
                        .setReadHyperlinks(true) // NOTE: Needed for copying rows.
                        .setReadSharedFormulas(true) // NOTE: If not set to true, then data with shared formulas fail.
                        .open(in);

                int index = 0;
                for (final Sheet originalSheet : originalWorkbook) {
                    final String originalSheetName = originalSheet.getSheetName();

                    try (final SXSSFWorkbook newWorkbook = new SXSSFWorkbook(null, SXSSFWorkbook.DEFAULT_WINDOW_SIZE, false, true)) {
                        final SXSSFSheet newSheet = newWorkbook.createSheet(originalSheetName);
                        final int numberOfCopiedRows = copyRows(originalSheet, newSheet);

                        final FlowFile newFlowFile = session.create(originalFlowFile);
                        try (final OutputStream out = session.write(newFlowFile)) {
                            newWorkbook.write(out);
                            workbookSplits.add(new WorkbookSplit(index, newFlowFile, originalSheetName, numberOfCopiedRows));
                        }
                    }

                    index++;
                }
            });
        } catch (ExcelRuntimeException | IllegalStateException | ProcessException e) {
            getLogger().error("Failed to split {}", originalFlowFile, e);
            session.remove(workbookSplits.stream()
                    .map(WorkbookSplit::content)
                    .toList());
            workbookSplits.clear();
            session.transfer(originalFlowFile, REL_FAILURE);
            return;
        }

        final String fragmentId = UUID.randomUUID().toString();
        final String originalFileName = originalFlowFile.getAttribute(CoreAttributes.FILENAME.key());
        final int extensionIndex = originalFileName.lastIndexOf(".");
        String originalFileNameWithoutExtension = originalFileName;
        String originalFileNameExtension = "";

        if (extensionIndex > -1) {
            originalFileNameWithoutExtension = originalFileName.substring(0, extensionIndex);
            originalFileNameExtension = originalFileName.substring(extensionIndex);
        }

        final Map<String, String> attributes = new HashMap<>();
        attributes.put(FRAGMENT_COUNT.key(), String.valueOf(workbookSplits.size()));
        attributes.put(FRAGMENT_ID.key(), fragmentId);
        attributes.put(SEGMENT_ORIGINAL_FILENAME.key(), originalFileName);

        for (WorkbookSplit split : workbookSplits) {
            attributes.put(CoreAttributes.FILENAME.key(), String.format("%s-%s%s", originalFileNameWithoutExtension, split.index(), originalFileNameExtension));
            attributes.put(FRAGMENT_INDEX.key(), Integer.toString(split.index()));
            attributes.put(SHEET_NAME, split.sheetName());
            attributes.put(TOTAL_ROWS, Integer.toString(split.numRows()));
            session.putAllAttributes(split.content(), attributes);
        }

        session.transfer(originalFlowFile, REL_ORIGINAL);
        final List<FlowFile> flowFileSplits = workbookSplits.stream()
                .map(WorkbookSplit::content)
                .toList();

        session.transfer(flowFileSplits, REL_SPLIT);
    }

    private int copyRows(final Sheet originalSheet, final SXSSFSheet destinationSheet) {
        final CellCopyContext cellCopyContext = new CellCopyContext();
        int rowCount = 0;

        for (final Row sourceRow : originalSheet) {
            final Row destinationRow = destinationSheet.createRow(sourceRow.getRowNum());
            destinationRow.setHeight(sourceRow.getHeight());

            for (final Cell sourceCell : sourceRow) {
                final Cell destCell = destinationRow.createCell(sourceCell.getColumnIndex());
                CellUtil.copyCell(sourceCell, destCell, CELL_COPY_POLICY, cellCopyContext);
            }

            rowCount++;
        }

        for (final CellRangeAddress sourceRegion : originalSheet.getMergedRegions()) {
            destinationSheet.addMergedRegion(sourceRegion.copy());
        }

        for (final Hyperlink hyperlink : originalSheet.getHyperlinkList()) {
            destinationSheet.addHyperlink(((XlsxHyperlink) hyperlink).createXSSFHyperlink());
        }

        return rowCount;
    }

    private record WorkbookSplit(int index, FlowFile content, String sheetName, int numRows) { }
}
