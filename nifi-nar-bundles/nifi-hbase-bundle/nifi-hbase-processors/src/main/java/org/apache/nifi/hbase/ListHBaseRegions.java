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

import org.apache.commons.lang3.StringUtils;
import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.behavior.WritesAttributes;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.hbase.scan.HBaseRegion;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Set;

@InputRequirement(InputRequirement.Requirement.INPUT_FORBIDDEN)
@Tags({"hbase", "regions", "scan", "rowkey"})
@CapabilityDescription("Returns the information about the regions of an HBase table, including ID, name and row key ranges. " +
        "This information is helpful to feed into start row key and end row key for scans to HBase, e.g. using the ScanHBase processor.")
@WritesAttributes({
        @WritesAttribute(attribute = "hbase.region.name", description = "The name of the HBase region."),
        @WritesAttribute(attribute = "hbase.region.id", description = "The id of the HBase region."),
        @WritesAttribute(attribute = "hbase.region.startRowKey", description = "The starting row key (inclusive) of the HBase region. " +
                "The bytes returned from HBase is converted into a UTF-8 encoded string."),
        @WritesAttribute(attribute = "hbase.region.endRowKey", description = "The ending row key (exclusive) of the HBase region. " +
                "The bytes returned from HBase is converted into a UTF-8 encoded string.")
})
public class ListHBaseRegions extends AbstractProcessor {
    static final String HBASE_REGION_NAME_ATTR = "hbase.region.name";
    static final String HBASE_REGION_ID_ATTR = "hbase.region.id";
    static final String HBASE_REGION_START_ROW_ATTR = "hbase.region.startRowKey";
    static final String HBASE_REGION_END_ROW_ATTR = "hbase.region.endRowKey";
    static final PropertyDescriptor HBASE_CLIENT_SERVICE = new PropertyDescriptor.Builder()
            .name("HBase Client Service")
            .description("Specifies the Controller Service to use for accessing HBase.")
            .required(true)
            .identifiesControllerService(HBaseClientService.class)
            .build();

    static final PropertyDescriptor TABLE_NAME = new PropertyDescriptor.Builder()
            .name("Table Name")
            .description("The name of the HBase Table to put data into")
            .required(true)
            .expressionLanguageSupported(ExpressionLanguageScope.ENVIRONMENT)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    static final PropertyDescriptor ROUTE_DEGENERATE_REGIONS = new PropertyDescriptor.Builder()
            .name("Route Degenerate Regions")
            .required(false)
            .defaultValue("false")
            .allowableValues("true", "false")
            .addValidator(StandardValidators.BOOLEAN_VALIDATOR)
            .build();

    static final Relationship REL_SUCCESS = new Relationship.Builder()
            .name("success")
            .description("FlowFiles with information on regions of the HBase table are routed to this relationship.")
            .build();

    static final Relationship REL_DEGENERATE = new Relationship.Builder()
            .name("degenerate")
            .description("If \\\"Route Degenerate Regions\\\" is set, any " +
                    "FlowFile(s) that contains information about a region that is degenerate will be routed " +
                    "to this relationship. Otherwise, they will be sent to the success relationship.")
            .autoTerminateDefault(true)
            .build();

    @Override
    public Set<Relationship> getRelationships() {
        return Set.of(REL_SUCCESS, REL_DEGENERATE);
    }

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return List.of(
                HBASE_CLIENT_SERVICE,
                TABLE_NAME,
                ROUTE_DEGENERATE_REGIONS
        );
    }

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {
        final String tableName = context.getProperty(TABLE_NAME).evaluateAttributeExpressions().getValue();
        if (StringUtils.isBlank(tableName)) {
            getLogger().error("Table Name is blank or null, no regions information to be fetched.");
            context.yield();
            return;
        }

        final HBaseClientService hBaseClientService = context.getProperty(HBASE_CLIENT_SERVICE).asControllerService(HBaseClientService.class);

        final boolean routeDegenerateRegions = context.getProperty(ROUTE_DEGENERATE_REGIONS).asBoolean();

        try {
            final List<HBaseRegion> hBaseRegions = hBaseClientService.listHBaseRegions(tableName);
            for (final HBaseRegion region : hBaseRegions) {
                final FlowFile flowFile = session.create();
                session.putAttribute(flowFile, HBASE_REGION_NAME_ATTR, region.getRegionName());
                session.putAttribute(flowFile, HBASE_REGION_ID_ATTR, String.valueOf(region.getRegionId()));
                if (region.getStartRowKey() == null) {
                    session.putAttribute(flowFile, HBASE_REGION_START_ROW_ATTR, "");
                } else {
                    session.putAttribute(flowFile, HBASE_REGION_START_ROW_ATTR, new String(region.getStartRowKey(), StandardCharsets.UTF_8));
                }

                if (region.getEndRowKey() == null) {
                    session.putAttribute(flowFile, HBASE_REGION_END_ROW_ATTR, "");
                } else {
                    session.putAttribute(flowFile, HBASE_REGION_END_ROW_ATTR, new String(region.getEndRowKey(), StandardCharsets.UTF_8));
                }

                if (region.isDegenerate() && routeDegenerateRegions) {
                    getLogger().warn("Region with id {} and name {} is degenerate. Routing to degenerate relationship.", region.getRegionId(), region.getRegionName());
                    session.transfer(flowFile, REL_DEGENERATE);
                } else {
                    session.transfer(flowFile, REL_SUCCESS);
                }
            }
        } catch (final HBaseClientException e) {
            getLogger().error("Failed to receive information on HBase regions for table {} due to {}", tableName, e);
            context.yield();
            throw new RuntimeException(e);
        }
    }
}
