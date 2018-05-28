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
package org.apache.nifi.hbase;

import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.behavior.WritesAttributes;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.util.StandardValidators;

import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Scanner;

@InputRequirement(InputRequirement.Requirement.INPUT_REQUIRED)
@WritesAttributes({
        @WritesAttribute(attribute = "error.line", description = "The line number of the error."),
        @WritesAttribute(attribute = "error.msg", description = "The message explaining the error.")
})
@Tags({"hbase", "delete", "cell", "cells", "visibility"})
@CapabilityDescription("This processor allows the user to delete individual HBase cells by specifying one or more lines " +
        "in the flowfile content that are a sequence composed of row ID, column family, column qualifier and associated visibility labels " +
        "if visibility labels are enabled and in use. A user-defined separator is used to separate each of these pieces of data on each " +
        "line, with :::: being the default separator.")
public class DeleteHBaseCells extends AbstractDeleteHBase {
    static final PropertyDescriptor SEPARATOR = new PropertyDescriptor.Builder()
            .name("delete-hbase-cell-separator")
            .displayName("Separator")
            .description("Each line of the flowfile content is separated into components for building a delete using this" +
                    "separator. It should be something other than a single colon or a comma because these are values that " +
                    "are associated with columns and visibility labels respectively. To delete a row with ID xyz, column family abc, " +
                    "column qualifier def and visibility label PII&PHI, one would specify xyz::::abc::::def::::PII&PHI given the default " +
                    "value")
            .required(true)
            .defaultValue("::::")
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .build();

    static final String ERROR_LINE = "error.line";
    static final String ERROR_MSG  = "error.msg";

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        final List<PropertyDescriptor> properties = new ArrayList<>();
        properties.add(HBASE_CLIENT_SERVICE);
        properties.add(TABLE_NAME);
        properties.add(SEPARATOR);

        return properties;
    }

    private FlowFile writeErrorAttributes(int line, String msg, FlowFile file, ProcessSession session) {
        file = session.putAttribute(file, ERROR_LINE, String.valueOf(line));
        file = session.putAttribute(file, ERROR_MSG, msg != null ? msg : "");
        return file;
    }

    private void logCell(String rowId, String family, String column, String visibility) {
        StringBuilder sb = new StringBuilder()
            .append("Assembling cell delete for...\t")
            .append(String.format("Row ID: %s\t", rowId))
            .append(String.format("Column Family: %s\t", family))
            .append(String.format("Column Qualifier: %s\t", column))
            .append(String.format("Visibility Label: %s", visibility));
        getLogger().debug(sb.toString());
    }

    @Override
    protected void doDelete(ProcessContext context, ProcessSession session) throws Exception {
        FlowFile input = session.get();
        if (input == null) {
            return;
        }

        final String separator = context.getProperty(SEPARATOR).evaluateAttributeExpressions(input).getValue();
        final String tableName = context.getProperty(TABLE_NAME).evaluateAttributeExpressions(input).getValue();
        List<String> rowKeys = new ArrayList<>();
        int lineNum = 1;
        try (InputStream is = session.read(input)) {
            Scanner scanner = new Scanner(is);
            List<DeleteRequest> deletes = new ArrayList<>();
            while (scanner.hasNextLine()) {
                String line = scanner.nextLine().trim();
                if (line.equals("")) {
                    continue;
                }
                String[] parts = line.split(separator);
                if (parts.length < 3 || parts.length > 4) {
                    final String msg = String.format("Invalid line length. It must have 3 or 4 components. It had %d.", parts.length);
                    input = writeErrorAttributes(lineNum, msg, input, session);
                    session.transfer(input, REL_FAILURE);
                    getLogger().error(msg);
                    return;
                }
                String rowId = parts[0];
                String family = parts[1];
                String column = parts[2];
                String visibility = parts.length == 4 ? parts[3] : null;

                DeleteRequest request = new DeleteRequest(rowId.getBytes(), family.getBytes(), column.getBytes(), visibility);
                deletes.add(request);
                if (!rowKeys.contains(rowId)) {
                    rowKeys.add(rowId);
                }

                if (getLogger().isDebugEnabled()) {
                    logCell(rowId, family, column, visibility);
                }

                lineNum++;
            }
            is.close();
            clientService.deleteCells(tableName, deletes);
            for (int index = 0; index < rowKeys.size(); index++) { //Could be many row keys in one flowfile.
                session.getProvenanceReporter().invokeRemoteProcess(input, clientService.toTransitUri(tableName, rowKeys.get(index)));
            }

            session.transfer(input, REL_SUCCESS);
        } catch (Exception ex) {
            input = writeErrorAttributes(lineNum, ex.getMessage(), input, session);
            session.transfer(input, REL_FAILURE);
        }
    }
}
