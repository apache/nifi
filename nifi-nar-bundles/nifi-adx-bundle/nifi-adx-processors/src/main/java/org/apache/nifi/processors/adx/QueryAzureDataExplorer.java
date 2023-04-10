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
package org.apache.nifi.processors.adx;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.microsoft.azure.kusto.data.Client;
import com.microsoft.azure.kusto.data.KustoOperationResult;
import com.microsoft.azure.kusto.data.exceptions.DataClientException;
import com.microsoft.azure.kusto.data.exceptions.DataServiceException;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.nifi.adx.AdxSourceConnectionService;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.behavior.WritesAttributes;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.ProcessorInitializationContext;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.processors.adx.enums.AzureAdxSourceProcessorParameter;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.List;
import java.util.Set;

@Tags({"azure", "adx", "microsoft", "data", "explorer", "source"})
@CapabilityDescription("This Processor acts as a ADX source connector which queries data from Azure Data Explorer."+
        "This connector can act only as a start of the data pipeline getting data from ADX."+
        "The queries which can be used further details can be found here https://learn.microsoft.com/en-us/azure/data-explorer/kusto/concepts/querylimits")
@WritesAttributes({
        @WritesAttribute(attribute = "ADX_QUERY_ERROR_MESSAGE", description = "Azure Data Explorer error message."),
        @WritesAttribute(attribute = "ADX_EXECUTED_QUERY", description = "Azure Data Explorer executed query.")
})
public class QueryAzureDataExplorer extends AbstractProcessor {
    public static final String ADX_QUERY_ERROR_MESSAGE = "adx.query.error.message";
    public static final String ADX_EXECUTED_QUERY = "adx.executed.query";
    public static final String RELATIONSHIP_SUCCESS = "SUCCESS";

    public static final String RELATIONSHIP_FAILED = "FAILED";
    public static final String RELATIONSHIP_FAILED_DESC = "Relationship for failure";
    public static final String RELATIONSHIP_SUCCESS_DESC = "Relationship for success";

    public static final Relationship SUCCESS = new Relationship.Builder()
            .name(RELATIONSHIP_SUCCESS)
            .description(RELATIONSHIP_SUCCESS_DESC)
            .build();
    public static final Relationship FAILED = new Relationship.Builder()
            .name(RELATIONSHIP_FAILED)
            .description(RELATIONSHIP_FAILED_DESC)
            .build();
    public static final PropertyDescriptor DB_NAME = new PropertyDescriptor
            .Builder().name(AzureAdxSourceProcessorParameter.DB_NAME.name())
            .displayName(AzureAdxSourceProcessorParameter.DB_NAME.getParamDisplayName())
            .description(AzureAdxSourceProcessorParameter.DB_NAME.getParamDescription())
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();
    public static final PropertyDescriptor ADX_QUERY = new PropertyDescriptor
            .Builder().name(AzureAdxSourceProcessorParameter.ADX_QUERY.name())
            .displayName(AzureAdxSourceProcessorParameter.ADX_QUERY.getParamDisplayName())
            .description(AzureAdxSourceProcessorParameter.ADX_QUERY.getParamDescription())
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();
    public static final PropertyDescriptor ADX_SOURCE_SERVICE = new PropertyDescriptor
            .Builder().name(AzureAdxSourceProcessorParameter.ADX_SOURCE_SERVICE.name())
            .displayName(AzureAdxSourceProcessorParameter.ADX_SOURCE_SERVICE.getParamDisplayName())
            .description(AzureAdxSourceProcessorParameter.ADX_SOURCE_SERVICE.getParamDescription())
            .required(true)
            .identifiesControllerService(AdxSourceConnectionService.class)
            .build();
    private final ObjectMapper objectMapper = new ObjectMapper();
    private Set<Relationship> relationships;
    private List<PropertyDescriptor> descriptors;
    private Client executionClient;

    @Override
    protected void init(final ProcessorInitializationContext context) {
        this.descriptors = List.of(ADX_SOURCE_SERVICE,DB_NAME,ADX_QUERY);
        this.relationships = Set.of(SUCCESS,FAILED);
    }

    @Override
    public Set<Relationship> getRelationships() {
        return this.relationships;
    }

    @Override
    public final List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return descriptors;
    }

    @OnScheduled
    public void onScheduled(final ProcessContext context) {
        AdxSourceConnectionService service = context.getProperty(ADX_SOURCE_SERVICE).asControllerService(AdxSourceConnectionService.class);
        executionClient = service.getKustoQueryClient();
    }

    @Override
    public void onTrigger(ProcessContext context, ProcessSession session) throws ProcessException {
        FlowFile outgoingFlowFile;
        String databaseName = context.getProperty(DB_NAME).getValue();
        String adxQuery;
        KustoOperationResult kustoOperationResult;

        //checks if this processor has any preceding connection, if yes retrieve
        if (context.hasIncomingConnection()) {
            FlowFile incomingFlowFile = session.get();
            //incoming connection exists but the incoming flowfile is null
            if (incomingFlowFile == null && context.hasNonLoopConnection()) {
                return;
            }
            //incoming connection exists and retrieve adxQuery from context
            if (incomingFlowFile != null && incomingFlowFile.getSize() == 0) {
                if (context.getProperty(ADX_QUERY).isSet()) {
                    adxQuery = context.getProperty(ADX_QUERY).evaluateAttributeExpressions(incomingFlowFile).getValue();
                } else {
                    String message = "FlowFile query is empty and no scheduled query is set";
                    getLogger().error(message);
                    incomingFlowFile = session.putAttribute(incomingFlowFile, ADX_QUERY_ERROR_MESSAGE, message);
                    session.transfer(incomingFlowFile, FAILED);
                    return;
                }
            } else {
                try {
                    adxQuery = getQuery(session, incomingFlowFile);
                } catch(IOException ioe) {
                    throw new ProcessException("Failed to read Query from FlowFile",ioe);
                }
            }
            outgoingFlowFile = incomingFlowFile;
        } else {
            outgoingFlowFile = session.create();
            adxQuery = context.getProperty(ADX_QUERY).evaluateAttributeExpressions(outgoingFlowFile).getValue();
        }

        try {
            //execute Query
            kustoOperationResult = executeQuery(databaseName,adxQuery);
            if(kustoOperationResult.getPrimaryResults() != null){
                List<List<Object>> tableData = kustoOperationResult.getPrimaryResults().getData();
                try(ByteArrayInputStream bais = new ByteArrayInputStream(objectMapper.writeValueAsBytes(tableData))){
                    session.importFrom(bais, outgoingFlowFile);
                }
            }
            //if no error
            outgoingFlowFile = session.putAttribute(outgoingFlowFile, ADX_EXECUTED_QUERY, String.valueOf(adxQuery));
            session.transfer(outgoingFlowFile, SUCCESS);
        } catch (DataServiceException | IOException | DataClientException e) {
            if(Arrays.stream(ExceptionUtils.getRootCauseStackTrace(e)).anyMatch(str -> str.contains("LimitsExceeded"))){
                getLogger().error("Exception occurred while reading data from ADX : Query Limits exceeded : Please modify your query to fetch results below the kusto query limits ", e);
            }else{
                getLogger().error("Exception occurred while reading data from ADX ", e);
            }
            session.transfer(outgoingFlowFile, FAILED);
        }
    }

    protected KustoOperationResult executeQuery(String databaseName, String adxQuery) throws DataServiceException, DataClientException {
        return executionClient.execute(databaseName,adxQuery);
    }

    protected String getQuery(final ProcessSession session, FlowFile incomingFlowFile) throws IOException {
        try (ByteArrayOutputStream baos = new ByteArrayOutputStream()) {
            session.exportTo(incomingFlowFile, baos);
            return baos.toString(StandardCharsets.UTF_8);
        }
    }
}
