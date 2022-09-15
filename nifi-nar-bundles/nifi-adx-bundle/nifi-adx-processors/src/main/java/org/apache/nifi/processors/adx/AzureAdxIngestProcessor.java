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

import org.apache.commons.lang3.StringUtils;
import org.apache.nifi.adx.AdxConnectionService;
import com.microsoft.azure.kusto.ingest.IngestClient;
import com.microsoft.azure.kusto.ingest.IngestionMapping;
import com.microsoft.azure.kusto.ingest.IngestionProperties;
import com.microsoft.azure.kusto.ingest.exceptions.IngestionClientException;
import com.microsoft.azure.kusto.ingest.exceptions.IngestionServiceException;
import com.microsoft.azure.kusto.ingest.result.IngestionResult;
import com.microsoft.azure.kusto.ingest.result.IngestionStatus;
import com.microsoft.azure.kusto.ingest.result.OperationStatus;
import com.microsoft.azure.kusto.ingest.source.StreamSourceInfo;
import com.microsoft.azure.storage.StorageException;
import org.apache.nifi.adx.AzureAdxConnectionService;
import org.apache.nifi.components.AllowableValue;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.annotation.behavior.ReadsAttribute;
import org.apache.nifi.annotation.behavior.ReadsAttributes;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.ProcessorInitializationContext;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.processors.adx.enums.AzureAdxIngestProcessorParamsEnum;
import org.apache.nifi.processors.adx.enums.DataFormatEnum;
import org.apache.nifi.processors.adx.enums.IngestionMappingKindEnum;
import org.apache.nifi.processors.adx.enums.IngestionReportLevelEnum;
import org.apache.nifi.processors.adx.enums.IngestionReportMethodEnum;
import org.apache.nifi.processors.adx.enums.IngestionStatusEnum;
import org.apache.nifi.processors.adx.enums.RelationshipStatusEnum;

import java.io.IOException;
import java.io.InputStream;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

@Tags({"azure", "adx", "microsoft", "data", "explorer"})
@CapabilityDescription("The Azure ADX Processor sends flowFiles using the ADX-Service to the provided Azure Data" +
        "Explorer Ingest Endpoint. The data can be sent through queued ingestion or streaming ingestion to the Azure Data Explorer cluster.")
@ReadsAttributes({
        @ReadsAttribute(attribute="DB_NAME", description="Specifies the name of the database where the data needs to be stored."),
        @ReadsAttribute(attribute="TABLE_NAME", description="Specifies the name of the table where the data needs to be stored."),
        @ReadsAttribute(attribute="MAPPING_NAME", description="Specifies the name of the mapping responsible for storing the data in appropriate columns."),
        @ReadsAttribute(attribute="FLUSH_IMMEDIATE", description="In case of queued ingestion, this property determines whether the data should be flushed immediately to the ingest endpoint."),
        @ReadsAttribute(attribute="DATA_FORMAT", description="Specifies the format of data that is send to Azure Data Explorer."),
        @ReadsAttribute(attribute="IM_KIND", description="Specifies the type of ingestion mapping related to the table in Azure Data Explorer."),
        @ReadsAttribute(attribute="IR_LEVEL", description="ADX can report events on several levels. Ex- None, Failure and Failure & Success."),
        @ReadsAttribute(attribute="IR_METHOD", description="ADX can report events on several methods. Ex- Table, Queue, Table&Queue.")
})
public class AzureAdxIngestProcessor extends AbstractProcessor {

    public static final AllowableValue AVRO = new AllowableValue(
            DataFormatEnum.AVRO.name(), DataFormatEnum.AVRO.getExtension(),
            DataFormatEnum.AVRO.getDescription());

    public static final AllowableValue APACHEAVRO = new AllowableValue(
            DataFormatEnum.APACHEAVRO.name(), DataFormatEnum.APACHEAVRO.getExtension(),
            DataFormatEnum.APACHEAVRO.getDescription());

    public static final AllowableValue CSV = new AllowableValue(
            DataFormatEnum.CSV.name(), DataFormatEnum.CSV.getExtension(),
            DataFormatEnum.CSV.getDescription());

    public static final AllowableValue JSON = new AllowableValue(
            DataFormatEnum.JSON.name(), DataFormatEnum.JSON.getExtension(),
            DataFormatEnum.JSON.getDescription());

    public static final AllowableValue MULTIJSON = new AllowableValue(
            DataFormatEnum.MULTIJSON.name(), DataFormatEnum.MULTIJSON.getExtension(),
            DataFormatEnum.MULTIJSON.getDescription());

    public static final AllowableValue ORC = new AllowableValue(
            DataFormatEnum.ORC.name(),DataFormatEnum.ORC.getExtension(),DataFormatEnum.ORC.getDescription());

    public static final AllowableValue PARQUET = new AllowableValue(
            DataFormatEnum.PARQUET.name(),DataFormatEnum.PARQUET.getExtension(),DataFormatEnum.PARQUET.getDescription());

    public static final AllowableValue PSV = new AllowableValue(
            DataFormatEnum.PSV.name(),DataFormatEnum.PSV.getExtension(),DataFormatEnum.PSV.getDescription());

    public static final AllowableValue SCSV = new AllowableValue(
            DataFormatEnum.SCSV.name(),DataFormatEnum.SCSV.getExtension(),DataFormatEnum.SCSV.getDescription());

    public static final AllowableValue SOHSV = new AllowableValue(
            DataFormatEnum.SOHSV.name(),DataFormatEnum.SOHSV.getExtension(),
            DataFormatEnum.SOHSV.getDescription());

    public static final AllowableValue TSV = new AllowableValue(
            DataFormatEnum.TSV.name(),DataFormatEnum.TSV.getExtension(),DataFormatEnum.TSV.getDescription());

    public static final AllowableValue TSVE = new AllowableValue(
            DataFormatEnum.TSVE.name(),DataFormatEnum.TSVE.getExtension(),
            DataFormatEnum.TSVE.getDescription());

    public static final AllowableValue TXT = new AllowableValue(
            DataFormatEnum.TXT.name(),DataFormatEnum.TXT.getExtension(),
            DataFormatEnum.TXT.getDescription());

    public static final AllowableValue IRL_NONE = new AllowableValue(
            IngestionReportLevelEnum.IRL_NONE.name(), IngestionReportLevelEnum.IRL_NONE.getIngestionReportLevel(),
            IngestionReportLevelEnum.IRL_NONE.getDescription());

    public static final AllowableValue IRL_FAIL = new AllowableValue(
            IngestionReportLevelEnum.IRL_FAIL.name(), IngestionReportLevelEnum.IRL_FAIL.getIngestionReportLevel(),
            IngestionReportLevelEnum.IRL_FAIL.getDescription());

    public static final AllowableValue IRL_FAS = new AllowableValue(
            IngestionReportLevelEnum.IRL_FAS.name(), IngestionReportLevelEnum.IRL_FAS.getIngestionReportLevel(),
            IngestionReportLevelEnum.IRL_FAS.getDescription());

    public static final AllowableValue IRM_QUEUE = new AllowableValue(
            IngestionReportMethodEnum.IRM_QUEUE.name(), IngestionReportMethodEnum.IRM_QUEUE.getIngestionReportMethod(),
            IngestionReportMethodEnum.IRM_QUEUE.getDescription());

    public static final AllowableValue IRM_TABLE = new AllowableValue(
            IngestionReportMethodEnum.IRM_TABLE.name(), IngestionReportMethodEnum.IRM_TABLE.getIngestionReportMethod(),
            IngestionReportMethodEnum.IRM_TABLE.getDescription());

    public static final AllowableValue IRM_TABLEANDQUEUE = new AllowableValue(
            IngestionReportMethodEnum.IRM_TABLEANDQUEUE.name(), IngestionReportMethodEnum.IRM_TABLEANDQUEUE.getIngestionReportMethod(),
            IngestionReportMethodEnum.IRM_TABLEANDQUEUE.getDescription());

    public static final AllowableValue IM_KIND_APACHEAVRO = new AllowableValue(
            IngestionMappingKindEnum.IM_KIND_APACHEAVRO.name(), IngestionMappingKindEnum.IM_KIND_APACHEAVRO.getMappingKind(),
            IngestionMappingKindEnum.IM_KIND_APACHEAVRO.getDescription());

    public static final AllowableValue IM_KIND_AVRO = new AllowableValue(
            IngestionMappingKindEnum.IM_KIND_AVRO.name(), IngestionMappingKindEnum.IM_KIND_AVRO.getMappingKind(),
            IngestionMappingKindEnum.IM_KIND_AVRO.getDescription());

    public static final AllowableValue IM_KIND_CSV = new AllowableValue(
            IngestionMappingKindEnum.IM_KIND_CSV.name(), IngestionMappingKindEnum.IM_KIND_CSV.getMappingKind(),
            IngestionMappingKindEnum.IM_KIND_CSV.getDescription());

    public static final AllowableValue IM_KIND_JSON = new AllowableValue(
            IngestionMappingKindEnum.IM_KIND_JSON.name(), IngestionMappingKindEnum.IM_KIND_JSON.getMappingKind(),
            IngestionMappingKindEnum.IM_KIND_JSON.getDescription());

    public static final AllowableValue IM_KIND_PARQUET = new AllowableValue(
            IngestionMappingKindEnum.IM_KIND_PARQUET.name(), IngestionMappingKindEnum.IM_KIND_PARQUET.getMappingKind(),
            IngestionMappingKindEnum.IM_KIND_PARQUET.getDescription());

    public static final AllowableValue IM_KIND_ORC = new AllowableValue(
            IngestionMappingKindEnum.IM_KIND_ORC.name(), IngestionMappingKindEnum.IM_KIND_ORC.getMappingKind(),
            IngestionMappingKindEnum.IM_KIND_ORC.getDescription());

    public static final AllowableValue ST_SUCCESS = new AllowableValue(
            IngestionStatusEnum.ST_SUCCESS.name(), IngestionStatusEnum.ST_SUCCESS.getIngestionStatus(),
            IngestionStatusEnum.ST_SUCCESS.getDescription());

    public static final AllowableValue ST_FIREANDFORGET = new AllowableValue(
            IngestionStatusEnum.ST_FIREANDFORGET.name(), IngestionStatusEnum.ST_FIREANDFORGET.getIngestionStatus(),
            IngestionStatusEnum.ST_FIREANDFORGET.getDescription());

    public static final PropertyDescriptor DB_NAME = new PropertyDescriptor
            .Builder().name(AzureAdxIngestProcessorParamsEnum.DB_NAME.name())
            .displayName(AzureAdxIngestProcessorParamsEnum.DB_NAME.getParamDisplayName())
            .description(AzureAdxIngestProcessorParamsEnum.DB_NAME.getParamDescription())
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final PropertyDescriptor TABLE_NAME = new PropertyDescriptor
            .Builder().name(AzureAdxIngestProcessorParamsEnum.TABLE_NAME.name())
            .displayName(AzureAdxIngestProcessorParamsEnum.TABLE_NAME.getParamDisplayName())
            .description(AzureAdxIngestProcessorParamsEnum.TABLE_NAME.getParamDescription())
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final PropertyDescriptor MAPPING_NAME = new PropertyDescriptor
            .Builder().name(AzureAdxIngestProcessorParamsEnum.MAPPING_NAME.name())
            .displayName(AzureAdxIngestProcessorParamsEnum.MAPPING_NAME.getParamDisplayName())
            .description(AzureAdxIngestProcessorParamsEnum.MAPPING_NAME.getParamDescription())
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    static final PropertyDescriptor FLUSH_IMMEDIATE = new PropertyDescriptor.Builder()
            .name(AzureAdxIngestProcessorParamsEnum.FLUSH_IMMEDIATE.name())
            .displayName(AzureAdxIngestProcessorParamsEnum.FLUSH_IMMEDIATE.getParamDisplayName())
            .description(AzureAdxIngestProcessorParamsEnum.FLUSH_IMMEDIATE.getParamDescription())
            .required(true)
            .defaultValue("false")
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .dependsOn(AzureAdxConnectionService.IS_STREAMING_ENABLED,"false")
            .build();

    static final PropertyDescriptor DATA_FORMAT = new PropertyDescriptor.Builder()
            .name(AzureAdxIngestProcessorParamsEnum.DATA_FORMAT.name())
            .displayName(AzureAdxIngestProcessorParamsEnum.DATA_FORMAT.getParamDisplayName())
            .description(AzureAdxIngestProcessorParamsEnum.DATA_FORMAT.getParamDescription())
            .required(true)
            .allowableValues(AVRO, APACHEAVRO, CSV, JSON, MULTIJSON, ORC, PARQUET, PSV, SCSV, SOHSV, TSV, TSVE, TXT)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    static final PropertyDescriptor IM_KIND = new PropertyDescriptor.Builder()
            .name(AzureAdxIngestProcessorParamsEnum.IM_KIND.name())
            .displayName(AzureAdxIngestProcessorParamsEnum.IM_KIND.getParamDisplayName())
            .description(AzureAdxIngestProcessorParamsEnum.IM_KIND.getParamDescription())
            .required(true)
            .allowableValues(IM_KIND_AVRO, IM_KIND_APACHEAVRO, IM_KIND_CSV, IM_KIND_JSON, IM_KIND_ORC, IM_KIND_PARQUET)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    static final PropertyDescriptor IR_LEVEL = new PropertyDescriptor.Builder()
            .name(AzureAdxIngestProcessorParamsEnum.IR_LEVEL.name())
            .displayName(AzureAdxIngestProcessorParamsEnum.IR_LEVEL.getParamDisplayName())
            .description(AzureAdxIngestProcessorParamsEnum.IR_LEVEL.getParamDescription())
            .required(true)
            .allowableValues(IRL_NONE, IRL_FAIL, IRL_FAS)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    static final PropertyDescriptor IR_METHOD = new PropertyDescriptor.Builder()
            .name(AzureAdxIngestProcessorParamsEnum.IR_METHOD.name())
            .displayName(AzureAdxIngestProcessorParamsEnum.IR_METHOD.getParamDisplayName())
            .description(AzureAdxIngestProcessorParamsEnum.IR_METHOD.getParamDescription())
            .required(true)
            .allowableValues(IRM_TABLE, IRM_QUEUE, IRM_TABLEANDQUEUE)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final PropertyDescriptor ADX_SERVICE = new PropertyDescriptor
            .Builder().name(AzureAdxIngestProcessorParamsEnum.ADX_SERVICE.name())
            .displayName(AzureAdxIngestProcessorParamsEnum.ADX_SERVICE.getParamDisplayName())
            .description(AzureAdxIngestProcessorParamsEnum.ADX_SERVICE.getParamDescription())
            .required(true)
            .identifiesControllerService(AdxConnectionService.class)
            .build();

    public static final PropertyDescriptor WAIT_FOR_STATUS = new PropertyDescriptor
            .Builder().name(AzureAdxIngestProcessorParamsEnum.WAIT_FOR_STATUS.name())
            .displayName(AzureAdxIngestProcessorParamsEnum.WAIT_FOR_STATUS.getParamDisplayName())
            .description(AzureAdxIngestProcessorParamsEnum.WAIT_FOR_STATUS.getParamDescription())
            .required(true)
            .allowableValues(ST_SUCCESS, ST_FIREANDFORGET)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final Relationship RL_SUCCEEDED = new Relationship.Builder()
            .name(RelationshipStatusEnum.RL_SUCCEEDED.name())
            .description(RelationshipStatusEnum.RL_SUCCEEDED.getDescription())
            .build();

    public static final Relationship RL_FAILED = new Relationship.Builder()
            .name(RelationshipStatusEnum.RL_FAILED.name())
            .description(RelationshipStatusEnum.RL_FAILED.getDescription())
            .build();

    private List<PropertyDescriptor> descriptors;

    private Set<Relationship> relationships;

    private AdxConnectionService service;

    private IngestClient ingestClient;

    @Override
    protected void init(final ProcessorInitializationContext context) {
        final List<PropertyDescriptor> descriptors = new ArrayList<>();
        descriptors.add(ADX_SERVICE);
        descriptors.add(DB_NAME);
        descriptors.add(TABLE_NAME);
        descriptors.add(MAPPING_NAME);
        descriptors.add(FLUSH_IMMEDIATE);
        descriptors.add(DATA_FORMAT);
        descriptors.add(IR_LEVEL);
        descriptors.add(IR_METHOD);
        descriptors.add(IM_KIND);
        descriptors.add(WAIT_FOR_STATUS);
        this.descriptors = Collections.unmodifiableList(descriptors);

        final Set<Relationship> relationships = new HashSet<>();
        relationships.add(RL_SUCCEEDED);
        relationships.add(RL_FAILED);
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

    @OnScheduled
    public void onScheduled(final ProcessContext context) {
        service = context.getProperty(ADX_SERVICE).asControllerService(AdxConnectionService.class);
        ingestClient = service.getAdxClient();
    }

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {

        FlowFile flowFile = session.get();
        if ( flowFile == null ) {
            context.yield();
            return;
        }

        try (final InputStream in = session.read(flowFile)) {

            IngestionProperties ingestionProperties = new IngestionProperties(context.getProperty(DB_NAME).getValue(),
                    context.getProperty(TABLE_NAME).getValue());


            switch(IngestionMappingKindEnum.valueOf(context.getProperty(IM_KIND).getValue()) ) {
                case IM_KIND_AVRO: ingestionProperties.setIngestionMapping(context.getProperty(MAPPING_NAME).getValue(),
                        IngestionMapping.IngestionMappingKind.AVRO); break;
                case IM_KIND_APACHEAVRO: ingestionProperties.setIngestionMapping(context.getProperty(MAPPING_NAME).getValue(),
                        IngestionMapping.IngestionMappingKind.APACHEAVRO); break;
                case IM_KIND_CSV: ingestionProperties.setIngestionMapping(context.getProperty(MAPPING_NAME).getValue(),
                        IngestionMapping.IngestionMappingKind.CSV); break;
                case IM_KIND_JSON: ingestionProperties.setIngestionMapping(context.getProperty(MAPPING_NAME).getValue(),
                        IngestionMapping.IngestionMappingKind.JSON); break;
                case IM_KIND_ORC: ingestionProperties.setIngestionMapping(context.getProperty(MAPPING_NAME).getValue(),
                        IngestionMapping.IngestionMappingKind.ORC); break;
                case IM_KIND_PARQUET: ingestionProperties.setIngestionMapping(context.getProperty(MAPPING_NAME).getValue(),
                        IngestionMapping.IngestionMappingKind.PARQUET); break;
            }

            switch(DataFormatEnum.valueOf(context.getProperty(DATA_FORMAT).getValue())) {
                case AVRO : ingestionProperties.setDataFormat(IngestionProperties.DataFormat.AVRO); break;
                case APACHEAVRO: ingestionProperties.setDataFormat(IngestionProperties.DataFormat.APACHEAVRO); break;
                case CSV: ingestionProperties.setDataFormat(IngestionProperties.DataFormat.CSV); break;
                case JSON: ingestionProperties.setDataFormat(IngestionProperties.DataFormat.JSON); break;
                case MULTIJSON: ingestionProperties.setDataFormat(IngestionProperties.DataFormat.MULTIJSON); break;
                case ORC: ingestionProperties.setDataFormat(IngestionProperties.DataFormat.ORC); break;
                case PARQUET: ingestionProperties.setDataFormat(IngestionProperties.DataFormat.PARQUET); break;
                case PSV: ingestionProperties.setDataFormat(IngestionProperties.DataFormat.PSV); break;
                case SCSV: ingestionProperties.setDataFormat(IngestionProperties.DataFormat.SCSV); break;
                case SOHSV: ingestionProperties.setDataFormat(IngestionProperties.DataFormat.SOHSV); break;
                case TSV: ingestionProperties.setDataFormat(IngestionProperties.DataFormat.TSV); break;
                case TSVE: ingestionProperties.setDataFormat(IngestionProperties.DataFormat.TSVE); break;
                case TXT: ingestionProperties.setDataFormat(IngestionProperties.DataFormat.TXT); break;
            }

            switch(IngestionReportLevelEnum.valueOf(context.getProperty(IR_LEVEL).getValue())) {
                case IRL_NONE: ingestionProperties.setReportLevel(IngestionProperties.IngestionReportLevel.NONE); break;
                case IRL_FAIL: ingestionProperties.setReportLevel(IngestionProperties.IngestionReportLevel.FAILURES_ONLY); break;
                case IRL_FAS: ingestionProperties.setReportLevel(IngestionProperties.IngestionReportLevel.FAILURES_AND_SUCCESSES); break;
            }

            switch (IngestionReportMethodEnum.valueOf(context.getProperty(IR_METHOD).getValue())) {
                case IRM_TABLE: ingestionProperties.setReportMethod(IngestionProperties.IngestionReportMethod.TABLE); break;
                case IRM_QUEUE: ingestionProperties.setReportMethod(IngestionProperties.IngestionReportMethod.QUEUE); break;
                case IRM_TABLEANDQUEUE: ingestionProperties.setReportMethod(IngestionProperties.IngestionReportMethod.QUEUE_AND_TABLE); break;
            }

            if (StringUtils.equalsIgnoreCase(context.getProperty(FLUSH_IMMEDIATE).getValue(),"true")) {
                ingestionProperties.setFlushImmediately(true);
            } else {
                ingestionProperties.setFlushImmediately(false);
            }

            StringBuilder ingestLogString = new StringBuilder().append("Ingesting with: ")
                            .append("dataFormat - ").append(ingestionProperties.getDataFormat()).append("|")
                            .append("ingestionMapping - ").append(ingestionProperties.getIngestionMapping().getIngestionMappingReference()).append("|")
                            .append("reportLevel - ").append(ingestionProperties.getReportLevel()).append("|")
                            .append("reportMethod - ").append(ingestionProperties.getReportMethod()).append("|")
                            .append("databaseName - ").append(ingestionProperties.getDatabaseName()).append("|")
                            .append("tableName - ").append(ingestionProperties.getTableName()).append("|")
                            .append("flushImmediately - ").append(ingestionProperties.getFlushImmediately());


            getLogger().info(ingestLogString.toString());

            StreamSourceInfo info = new StreamSourceInfo(in);

            IngestionResult result = ingestClient.ingestFromStream(info, ingestionProperties);

            List<IngestionStatus> statuses = result.getIngestionStatusCollection();

            if(StringUtils.equalsIgnoreCase(context.getProperty(WAIT_FOR_STATUS).getValue(),IngestionStatusEnum.ST_SUCCESS.name())) {
                while (statuses.get(0).status == OperationStatus.Pending) {
                    Thread.sleep(50);
                    statuses = result.getIngestionStatusCollection();
                    if(statuses.get(0).status == OperationStatus.Succeeded || statuses.get(0).status == OperationStatus.Failed) {
                        break;
                    }
                }
            } else {
                IngestionStatus status = new IngestionStatus();
                status.status = OperationStatus.Succeeded;
                statuses.set(0, status);
            }

            getLogger().info("Operation status: {} ", statuses.get(0).details);

            if(statuses.get(0).status == OperationStatus.Succeeded) {
                getLogger().info("Operation status Succedded - {}",statuses.get(0).status.toString());
                session.transfer(flowFile, RL_SUCCEEDED);
            }

            if(statuses.get(0).status == OperationStatus.Failed) {
                getLogger().error("Operation status Error - {}",statuses.get(0).status.toString());
                session.transfer(flowFile, RL_FAILED);
            }

        } catch (IOException | IngestionClientException | IngestionServiceException | StorageException | URISyntaxException | InterruptedException e) {
            getLogger().error("Exception occurred while ingesting data into ADX with exception {} ",e);
            throw new ProcessException(e);
        }

    }
}
