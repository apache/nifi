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
package org.apache.nifi.processors.deltalake;

import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.TriggerSerially;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.behavior.WritesAttributes;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.components.AllowableValue;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.resource.ResourceCardinality;
import org.apache.nifi.components.resource.ResourceType;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.flowfile.attributes.CoreAttributes;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.ProcessorInitializationContext;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.JsonValidator;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.processors.deltalake.service.DeltaLakeService;
import org.apache.nifi.processors.deltalake.service.DeltaLakeServiceImpl;

import java.io.InputStream;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

@TriggerSerially
@Tags({"deltalake", "deltatable", "cloud", "storage", "parquet", "writer"})
@InputRequirement(InputRequirement.Requirement.INPUT_ALLOWED)
@CapabilityDescription("Creates or updates a Delta table. Supports various storage solutions.")
@WritesAttributes({
@WritesAttribute(attribute = "mime.type", description = "application/json"),
        @WritesAttribute(attribute = "number.of.new.files", description = "Returns how many new files has been successfully added to the Delta table"),
        @WritesAttribute(attribute = "number.of.files.removed", description = "Returns how many files has been successfully removed from the Delta table"),
        @WritesAttribute(attribute = "number.of.files.in.the.new.deltatable",
                description = "If there were no Delta table, a new table is created and this attribute will contain the number of files the new table has"),
})
public class UpdateDeltaLakeTable extends AbstractProcessor {

    public static final AllowableValue LOCAL_FILESYSTEM = new AllowableValue("LOCAL", "Local Filesystem",
            "The parquet files stored on the local filesystem.");

    public static final AllowableValue AMAZON_S3 = new AllowableValue("S3", "Amazon S3",
            "The parquet files stored in a AWS S3 bucket.");

    public static final AllowableValue MICROSOFT_AZURE = new AllowableValue("AZURE", "Microsoft Azure",
            "The parquet files stored in a Microsoft Azure blob.");

    public static final AllowableValue GCP = new AllowableValue("GCP", "GCP",
            "The parquet files stored in a GCP bucket.");

    public static final AllowableValue PARQUET_SCHEMA_INPUT_TEXT = new AllowableValue("INPUT", "Text Input",
            "The parquet files schema in text input in JSON format");

    public static final AllowableValue PARQUET_SCHEMA_FILE = new AllowableValue("FILE", "Local file path",
            "The parquet schema files path");

    private List<PropertyDescriptor> descriptors;

    private Set<Relationship> relationships;

    public static final PropertyDescriptor HADOOP_CONFIGURATION_RESOURCES = new PropertyDescriptor.Builder()
            .name("hadoop-config-resources")
            .displayName("Hadoop Configuration Resources")
            .description("A file, or comma separated list of files, which contain the Hadoop configuration (core-site.xml, etc.)")
            .identifiesExternalResource(ResourceCardinality.MULTIPLE, ResourceType.FILE)
            .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
            .required(false)
            .build();

    public static final PropertyDescriptor INPUT_PARTITION_VALUES = new PropertyDescriptor.Builder()
            .name("input-file-partition-values")
            .displayName("Partition Values Of The Input File")
            .description("If the input file is partitioned, this should be filled in. Example values: \"2020,555\"," +
                    " where the corresponding partition columns are the followings: \"year,id\"")
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
            .required(false)
            .build();

    public static final PropertyDescriptor STORAGE_SELECTOR = new PropertyDescriptor.Builder()
            .name("storage-location-selector")
            .displayName("Parquet Storage Location")
            .description("Choose storage provider where the parquet files stored")
            .allowableValues(LOCAL_FILESYSTEM, AMAZON_S3, MICROSOFT_AZURE, GCP)
            .defaultValue(LOCAL_FILESYSTEM.getValue())
            .required(true)
            .build();

    public static final PropertyDescriptor PARQUET_SCHEMA_SELECTOR = new PropertyDescriptor.Builder()
            .name("parquet-schema-selector")
            .displayName("Parquet Schema Location")
            .description("Choose parquet schema source. Text input or local file.")
            .allowableValues(PARQUET_SCHEMA_INPUT_TEXT, PARQUET_SCHEMA_FILE)
            .defaultValue(PARQUET_SCHEMA_INPUT_TEXT.getValue())
            .required(true)
            .build();

    public static final PropertyDescriptor LOCAL_PATH = new PropertyDescriptor.Builder()
            .name("local-path")
            .displayName("Data Path On Local Filesystem")
            .description("Path on the local file system, can be absolute(has to start with '/') or relative path")
            .dependsOn(STORAGE_SELECTOR, LOCAL_FILESYSTEM)
            .addValidator(StandardValidators.createDirectoryExistsValidator(false, false))
            .required(true)
            .build();

    public static final PropertyDescriptor S3_ACCESS_KEY = new PropertyDescriptor.Builder()
            .name("s3-access-key")
            .displayName("S3 Access Key")
            .description("The access key for Amazon S3")
            .dependsOn(STORAGE_SELECTOR, AMAZON_S3)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .required(false)
            .sensitive(true)
            .build();

    public static final PropertyDescriptor S3_SECRET_KEY = new PropertyDescriptor.Builder()
            .name("s3-secret-key")
            .displayName("S3 Secret Key")
            .description("The secret key for Amazon S3")
            .dependsOn(STORAGE_SELECTOR, AMAZON_S3)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .required(false)
            .sensitive(true)
            .build();

    public static final PropertyDescriptor S3_BUCKET = new PropertyDescriptor.Builder()
            .name("s3-bucket-url")
            .displayName("S3 Bucket URL")
            .description("The bucket URL in S3, has to start with s3a://")
            .dependsOn(STORAGE_SELECTOR, AMAZON_S3)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .required(true)
            .build();

    public static final PropertyDescriptor S3_PATH = new PropertyDescriptor.Builder()
            .name("s3-data-path")
            .displayName("Data Path In S3 Bucket")
            .description("The path to the directory containing the parquet files within the S3 bucket")
            .dependsOn(STORAGE_SELECTOR, AMAZON_S3)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .required(true)
            .build();

    public static final PropertyDescriptor AZURE_ACCOUNT_KEY = new PropertyDescriptor.Builder()
            .name("azure-account-key")
            .displayName("Azure Account Key")
            .description("The account key for the Azure blob")
            .dependsOn(STORAGE_SELECTOR, MICROSOFT_AZURE)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .required(false)
            .sensitive(true)
            .build();

    public static final PropertyDescriptor AZURE_STORAGE_NAME = new PropertyDescriptor.Builder()
            .name("azure-storage-name")
            .displayName("Azure Blob Storage Name")
            .description("The storage name of the Azure blob")
            .dependsOn(STORAGE_SELECTOR, MICROSOFT_AZURE)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .required(true)
            .build();

    public static final PropertyDescriptor AZURE_STORAGE_ACCOUNT = new PropertyDescriptor.Builder()
            .name("azure-storage-account")
            .displayName("Azure Blob Storage Account")
            .description("The storage account for the Azure blob")
            .dependsOn(STORAGE_SELECTOR, MICROSOFT_AZURE)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .required(true)
            .build();

    public static final PropertyDescriptor AZURE_PATH = new PropertyDescriptor.Builder()
            .name("azure-data-path")
            .displayName("Data Path In Azure Blob")
            .description("The path to the directory containing the parquet files within the Azure blob")
            .dependsOn(STORAGE_SELECTOR, MICROSOFT_AZURE)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .required(true)
            .build();

    public static final PropertyDescriptor GCP_ACCOUNT_JSON_KEYFILE_PATH = new PropertyDescriptor.Builder()
            .name("gcp-keyfile-path")
            .displayName("GCP Account JSON Keyfile Path")
            .description("Local filesystem path to GCP account JSON keyfile, path has to contain the filename")
            .dependsOn(STORAGE_SELECTOR, GCP)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .identifiesExternalResource(ResourceCardinality.SINGLE, ResourceType.FILE)
            .required(false)
            .build();

    public static final PropertyDescriptor GCP_BUCKET = new PropertyDescriptor.Builder()
            .name("gcp-bucket-url")
            .displayName("GCP Bucket URL")
            .description("The GCP bucket URL, has to starts with gs://")
            .dependsOn(STORAGE_SELECTOR, GCP)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .required(true)
            .build();

    public static final PropertyDescriptor GCP_PATH = new PropertyDescriptor.Builder()
            .name("gcp-data-path")
            .displayName("Data Path In GCP Bucket")
            .description("The path to the directory containing the parquet files within the GCP bucket")
            .dependsOn(STORAGE_SELECTOR, GCP)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .required(true)
            .build();

    public static final PropertyDescriptor PARTITION_COLUMNS = new PropertyDescriptor.Builder()
            .name("partition-columns")
            .displayName("Parquet Partition Columns")
            .description("Parquet file partition columns, nested columns separated by ',' ")
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .required(false)
            .build();

    public static final PropertyDescriptor SCHEMA_TEXT_JSON = new PropertyDescriptor.Builder()
            .name("parquet-schema-text")
            .displayName("Parquet Schema In JSON Format")
            .description("Describes the data structure of the parquet file in JSON format as text")
            .addValidator(JsonValidator.INSTANCE)
            .dependsOn(PARQUET_SCHEMA_SELECTOR, PARQUET_SCHEMA_INPUT_TEXT)
            .required(true)
            .build();

    public static final PropertyDescriptor SCHEMA_FILE_JSON = new PropertyDescriptor.Builder()
            .name("parquet-schema-file")
            .displayName("Parquet Schema File Location")
            .description("Location of a JSON file that describes the data structure of the parquet file")
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .identifiesExternalResource(ResourceCardinality.SINGLE, ResourceType.FILE)
            .dependsOn(PARQUET_SCHEMA_SELECTOR, PARQUET_SCHEMA_FILE)
            .required(true)
            .build();

    public static final Relationship REL_SUCCESS = new Relationship.Builder()
            .name("success")
            .description("DeltaLake table successfully updated")
            .build();

    public static final Relationship REL_FAILURE = new Relationship.Builder()
            .name("failure")
            .description("DeltaLake table update failed")
            .build();

    private DeltaLakeService deltalakeService;

    @Override
    protected void init(final ProcessorInitializationContext context) {
        this.relationships = Collections.unmodifiableSet(new HashSet<>(Arrays.asList(REL_SUCCESS, REL_FAILURE)));
        this.descriptors = Collections.unmodifiableList(Arrays.asList(
                STORAGE_SELECTOR,
                HADOOP_CONFIGURATION_RESOURCES,
                LOCAL_PATH,
                PARQUET_SCHEMA_SELECTOR,
                SCHEMA_TEXT_JSON,
                SCHEMA_FILE_JSON,
                PARTITION_COLUMNS,
                INPUT_PARTITION_VALUES,
                S3_ACCESS_KEY,
                S3_SECRET_KEY,
                S3_BUCKET,
                S3_PATH,
                AZURE_ACCOUNT_KEY,
                AZURE_STORAGE_NAME,
                AZURE_STORAGE_ACCOUNT,
                AZURE_PATH,
                GCP_ACCOUNT_JSON_KEYFILE_PATH,
                GCP_BUCKET, GCP_PATH));
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
    public void onScheduled(ProcessContext processContext) {
        deltalakeService = new DeltaLakeServiceImpl(processContext);
    }

    @Override
    public void onTrigger(final ProcessContext processContext, final ProcessSession session) throws ProcessException {
        FlowFile file;
        file = session.get();
        if (file == null) {
            file = session.create();
        }
        try {
            if (processContext.hasIncomingConnection()) {
                handleInputFile(processContext, file, session.read(file));
            }
            Map<String, String> updateResult =  updateDeltaLake();
            FlowFile updatedFlowFIle = session.putAllAttributes(file, updateResult);
            session.transfer(updatedFlowFIle, REL_SUCCESS);
        } catch (Exception e) {
            getLogger().error("Error during delta table generation: ", e);
            session.transfer(file, REL_FAILURE);
        }
    }

    private void handleInputFile(ProcessContext processContext, FlowFile flowFile, InputStream read) {
        String filenameAttributeValue = flowFile.getAttribute(CoreAttributes.FILENAME.key());
        String partitionValues = processContext.getProperty(INPUT_PARTITION_VALUES).evaluateAttributeExpressions(flowFile).getValue();
        if (filenameAttributeValue != null) {
            deltalakeService.handleInputParquet(partitionValues, filenameAttributeValue, read);
        } else {
            throw new RuntimeException("No input file presented");
        }
    }

    private Map<String, String> updateDeltaLake() {
        Map<String, String> updateResult = new HashMap<>();
        if (deltalakeService.deltaTableExists()) {
            deltalakeService.startTransaction();
            int numberOfAddedFiles = deltalakeService.addNewFilesToDeltaTable();
            int numberOfRemovedFiles = deltalakeService.removeMissingFilesFromDeltaTable();
            deltalakeService.finishTransaction();
            updateResult.put("number.of.new.files", String.valueOf(numberOfAddedFiles));
            updateResult.put("number.of.files.removed", String.valueOf(numberOfRemovedFiles));
        } else {
            int filesInNewDeltaTable = deltalakeService.createNewDeltaLakeTable();
            updateResult.put("number.of.files.in.the.new.deltatable", String.valueOf(filesInNewDeltaTable));
        }
        return updateResult;
    }

}

