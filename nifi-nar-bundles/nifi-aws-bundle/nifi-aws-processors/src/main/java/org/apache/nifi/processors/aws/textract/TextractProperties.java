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

package org.apache.nifi.processors.aws.textract;

import com.amazonaws.services.textract.model.Block;
import com.amazonaws.services.textract.model.BlockType;
import org.apache.nifi.components.AllowableValue;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.util.StandardValidators;

import java.util.List;
import java.util.Map;

public class TextractProperties {
    static final String ACTION_ATTRIBUTE_NAME = "textract.action";

    // Actions to perform
    static final AllowableValue DETECT_TEXT = new AllowableValue("Detect Text", "Detect Text",
        "Analyzes JPEG or PNG image and extracts text from it");
    static final AllowableValue ANALYZE_DOCUMENT = new AllowableValue("Analyze Document", "Analyze Document",
        "Analyzes a document for \"relationships between detected items\"");
    static final AllowableValue ANALYZE_EXPENSE = new AllowableValue("Analyze Expense", "Analyze Expense",
        "Analyzes a document for \"financially related relationships between text\"");
    static final AllowableValue ANALYZE_ID = new AllowableValue("Analyze ID", "Analyze ID",
        "Analyzes a document for information related to a person's identity, such as analyzing a driver's license or passport");
    static final AllowableValue USE_ATTRIBUTE = new AllowableValue("Use 'textract.action' attribute", "Use 'textract.action' attribute",
        "Uses the value of the 'textract.action' attribute to determine which action to perform");

    // Analysis Options
    static final AllowableValue ANALYZE_TABLES = new AllowableValue("Analyze Tables", "Analyze Tables",
        "Tables that are encountered in the document will be analyzed but not forms.");
    static final AllowableValue ANALYZE_FORMS = new AllowableValue("Analyze Forms", "Analyze Forms",
        "Tables that are encountered in the document will be analyzed but not forms.");
    static final AllowableValue ANALYZE_TABLES_AND_FORMS = new AllowableValue("Analyze Tables and Forms", "Analyze Tables and Forms",
        "Both tables and forms that are encountered in the document will be analyzed.");


    static final PropertyDescriptor ACTION = new PropertyDescriptor.Builder()
        .name("Textract Action")
        .displayName("Textract Action")
        .description("Specifies which of the Textract Actions to perform")
        .allowableValues(DETECT_TEXT, ANALYZE_DOCUMENT, ANALYZE_EXPENSE, ANALYZE_ID, USE_ATTRIBUTE)
        .defaultValue(DETECT_TEXT.getValue())
        .required(true)
        .build();

    static final PropertyDescriptor ANALYSIS_FEATURES = new PropertyDescriptor.Builder()
        .name("Features to Analyze")
        .displayName("Features to Analyze")
        .description("Specifies which features should be analyzed. Note that the value selected here may affect the cost incurred by AWS.")
        .required(true)
        .allowableValues(ANALYZE_TABLES, ANALYZE_FORMS, ANALYZE_TABLES_AND_FORMS)
        .dependsOn(ACTION, TextractProperties.ANALYZE_DOCUMENT)
        .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
        .build();

    static final PropertyDescriptor LINE_ITEM_ATTRIBUTE_PREFIX = new PropertyDescriptor.Builder()
        .name("Line Item Attribute Prefix")
        .displayName("Line Item Attribute Prefix")
        .description("If specified, all line items that are identified in the document will be added as attributes with this prefix. For example, if this property is set to lineitem then attributes" +
            " will be added such as lineitem.0.computer=3000, lineitem.1.mouse=100 if the expense indicates a line item for a computer costing 3,000 and a line item for a mouse costing 100.")
        .required(false)
        .addValidator(StandardValidators.ATTRIBUTE_KEY_VALIDATOR)
        .expressionLanguageSupported(ExpressionLanguageScope.NONE)
        .dependsOn(TextractProperties.ACTION, TextractProperties.ANALYZE_EXPENSE)
        .build();
    static final PropertyDescriptor SUMMARY_ITEM_ATTRIBUTE_PREFIX = new PropertyDescriptor.Builder()
        .name("Summary Item Attribute Prefix")
        .displayName("Summary Item Attribute Prefix")
        .description("If specified, all summary items (information other than line items, such as vendor name) that are identified in the document will be added as attributes with this prefix. " +
            "For example, if this property is set to expense then attributes will be added such as expense.vendor_name=My Vendor.")
        .required(false)
        .addValidator(StandardValidators.ATTRIBUTE_KEY_VALIDATOR)
        .expressionLanguageSupported(ExpressionLanguageScope.NONE)
        .dependsOn(TextractProperties.ACTION, TextractProperties.ANALYZE_EXPENSE)
        .build();



    static String getAction(final ProcessContext context, final FlowFile flowFile) {
        final String actionName = context.getProperty(ACTION).getValue();
        if (USE_ATTRIBUTE.getValue().equalsIgnoreCase(actionName)) {
            return flowFile.getAttribute(ACTION_ATTRIBUTE_NAME);
        }

        return actionName;
    }

    static String lineBlocksToString(final List<Block> blocks, final int confidenceThreshold, final Map<String, String> attributes) {
        int textLinesFound = 0;
        int textLinesIncluded = 0;
        int textLinesExcluded = 0;

        final StringBuilder sb = new StringBuilder();
        for (final Block block : blocks) {
            final BlockType blockType = BlockType.valueOf(block.getBlockType());
            if (blockType != BlockType.LINE) {
                continue;
            }

            textLinesFound++;

            if (block.getConfidence() < confidenceThreshold) {
                textLinesExcluded++;
                continue;
            }

            textLinesIncluded++;
            final String text = block.getText();
            sb.append(text).append("\n");
        }

        sum(attributes, "text.lines.total", textLinesFound);
        sum(attributes, "text.lines.below.confidence.threshold", textLinesExcluded);
        sum(attributes, "text.lines.included", textLinesIncluded);

        attributes.put("text.confidence.threshold", String.valueOf(confidenceThreshold));
        return sb.toString();
    }

    private static void sum(final Map<String, String> attributes, final String attributeName, final int toAdd) {
        final String existingAttribute = attributes.get(attributeName);
        if (existingAttribute == null) {
            attributes.put(attributeName, String.valueOf(toAdd));
            return;
        }

        try {
            final int sum = Integer.parseInt(existingAttribute) + toAdd;
            attributes.put(attributeName, String.valueOf(sum));
        } catch (final NumberFormatException nfe) {
            attributes.put(attributeName, String.valueOf(toAdd));
        }
    }
}
