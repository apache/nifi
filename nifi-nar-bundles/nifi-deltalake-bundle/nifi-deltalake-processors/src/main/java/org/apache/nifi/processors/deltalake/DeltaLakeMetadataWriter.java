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
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.components.AllowableValue;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.ProcessorInitializationContext;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.processors.deltalake.service.DeltaLakeService;

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
public class DeltaLakeMetadataWriter extends AbstractProcessor {

    public static final AllowableValue LOCAL_FILESYSTEM = new AllowableValue("LOCAL", "Local Filesystem",
            "The parquet files stored on the local filesystem.");

    public static final AllowableValue AMAZON_S3 = new AllowableValue("S3", "Amazon S3",
            "The parquet files stored in a AWS S3 bucket.");

    public static final AllowableValue MICROSOFT_AZURE = new AllowableValue("AZURE", "Microsoft Azure",
            "The parquet files stored in a Microsoft Azure blob.");

    public static final AllowableValue GCP = new AllowableValue("GCP", "GCP",
            "The parquet files stored in a GCP bucket.");

    public static final AllowableValue PARQUET_SCHEMA_INPUT_TEXT = new AllowableValue("INPUT", "Text Input",
            "The parquet files schema in text input in json format");

    public static final AllowableValue PARQUET_SCHEMA_FILE = new AllowableValue("FILE", "Local file path",
            "The parquet schema files path");

    public static final AllowableValue NO_INPUT_FILE = new AllowableValue("INPUT_NO_FILE", "Don't process input file",
            "Don't import file from other processor");

    public static final AllowableValue PROCESS_INPUT_FILE = new AllowableValue("PROCESS_INPUT_FILE", "Process input file, reading path and filename from attribute",
            "Import new file from based on the given attributes.");

    private List<PropertyDescriptor> descriptors;

    private Set<Relationship> relationships;

    public static final PropertyDescriptor INPUT_FILE_SELECTOR = new PropertyDescriptor.Builder()
            .name("input-file-selector")
            .displayName("Input file")
            .description("Choose to process or not process the input file")
            .allowableValues(NO_INPUT_FILE, PROCESS_INPUT_FILE)
            .defaultValue(NO_INPUT_FILE.getValue())
            .required(true)
            .build();

    public static final PropertyDescriptor INPUT_FILE_PATH_ATTRIBUTE = new PropertyDescriptor.Builder()
            .name("input-file-path-attribute")
            .displayName("Attribute name of the input files paths")
            .description("Different processors give different attribute names for the output files path")
            .dependsOn(INPUT_FILE_SELECTOR, PROCESS_INPUT_FILE)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .required(true)
            .build();

    public static final PropertyDescriptor INPUT_FILE_NAME_ATTRIBUTE = new PropertyDescriptor.Builder()
            .name("input-file-name-attribute")
            .displayName("Attribute name of the input files name")
            .description("Different processors give different attribute names for the output filenames")
            .dependsOn(INPUT_FILE_SELECTOR, PROCESS_INPUT_FILE)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .required(true)
            .build();

    public static final PropertyDescriptor STORAGE_SELECTOR = new PropertyDescriptor.Builder()
            .name("storage-location-selector")
            .displayName("Parquet storage location")
            .description("Choose storage provider where the parquet files stored")
            .allowableValues(LOCAL_FILESYSTEM, AMAZON_S3, MICROSOFT_AZURE, GCP)
            .defaultValue(LOCAL_FILESYSTEM.getValue())
            .required(true)
            .build();

    public static final PropertyDescriptor PARQUET_SCHEMA_SELECTOR = new PropertyDescriptor.Builder()
            .name("parquet-schema-selector")
            .displayName("Parquet schema location")
            .description("Choose parquet schema source. Text input or local file.")
            .allowableValues(PARQUET_SCHEMA_INPUT_TEXT, PARQUET_SCHEMA_FILE)
            .defaultValue(PARQUET_SCHEMA_INPUT_TEXT.getValue())
            .required(true)
            .build();

    public static final PropertyDescriptor LOCAL_PATH = new PropertyDescriptor.Builder()
            .name("local-path")
            .displayName("Data path on local filesystem")
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
            .required(true)
            .sensitive(true)
            .build();

    public static final PropertyDescriptor S3_SECRET_KEY = new PropertyDescriptor.Builder()
            .name("s3-secret-key")
            .displayName("S3 Secret Key")
            .description("The secret key for Amazon S3")
            .dependsOn(STORAGE_SELECTOR, AMAZON_S3)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .required(true)
            .sensitive(true)
            .build();

    public static final PropertyDescriptor S3_BUCKET = new PropertyDescriptor.Builder()
            .name("s3-bucket-url")
            .displayName("S3 bucket url")
            .description("The bucket url in S3, has to start with s3a://")
            .dependsOn(STORAGE_SELECTOR, AMAZON_S3)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .required(true)
            .build();

    public static final PropertyDescriptor S3_PATH = new PropertyDescriptor.Builder()
            .name("s3-data-path")
            .displayName("Data path in S3 bucket")
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
            .required(true)
            .sensitive(true)
            .build();

    public static final PropertyDescriptor AZURE_STORAGE_NAME = new PropertyDescriptor.Builder()
            .name("azure-storage-name")
            .displayName("Azure blob storage name")
            .description("The storage name of the Azure blob")
            .dependsOn(STORAGE_SELECTOR, MICROSOFT_AZURE)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .required(true)
            .build();

    public static final PropertyDescriptor AZURE_STORAGE_ACCOUNT = new PropertyDescriptor.Builder()
            .name("azure-storage-account")
            .displayName("Azure blob storage account")
            .description("The storage account for the Azure blob")
            .dependsOn(STORAGE_SELECTOR, MICROSOFT_AZURE)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .required(true)
            .build();

    public static final PropertyDescriptor AZURE_PATH = new PropertyDescriptor.Builder()
            .name("azure-data-path")
            .displayName("Data path in Azure blob")
            .description("The path to the directory containing the parquet files within the Azure blob")
            .dependsOn(STORAGE_SELECTOR, MICROSOFT_AZURE)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .required(true)
            .build();

    public static final PropertyDescriptor GCP_ACCOUNT_JSON_KEYFILE_PATH = new PropertyDescriptor.Builder()
            .name("gcp-keyfile-path")
            .displayName("GCP account json keyfile path")
            .description("Local filesystem path to GCP account json keyfile, path has to contain the filename")
            .dependsOn(STORAGE_SELECTOR, GCP)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .required(true)
            .build();

    public static final PropertyDescriptor GCP_BUCKET = new PropertyDescriptor.Builder()
            .name("gcp-bucket-url")
            .displayName("GCP bucket url")
            .description("The GCP bucket url, has to starts with gs://")
            .dependsOn(STORAGE_SELECTOR, GCP)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .required(true)
            .build();

    public static final PropertyDescriptor GCP_PATH = new PropertyDescriptor.Builder()
            .name("gcp-data-path")
            .displayName("Data path in GCP bucket")
            .description("The path to the directory containing the parquet files within the GCP bucket")
            .dependsOn(STORAGE_SELECTOR, GCP)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .required(true)
            .build();

    public static final PropertyDescriptor PARTITION_COLUMNS = new PropertyDescriptor.Builder()
            .name("partition-columns")
            .displayName("Parquet partition columns")
            .description("Parquet file partition columns, nested columns separated by ',' ")
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .required(false)
            .build();

    public static final Relationship REL_SUCCESS = new Relationship.Builder()
            .name("success")
            .description("DeltaLake table successfully updated")
            .build();

    public static final Relationship REL_FAILED = new Relationship.Builder()
            .name("failure")
            .description("DeltaLake table update failed")
            .build();

    public static final PropertyDescriptor SCHEMA_TEXT_JSON = new PropertyDescriptor.Builder()
            .name("parquet-schema-text")
            .displayName("Parquet schema in json")
            .description("Describes the data structure of the parquet file in json format as text")
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .dependsOn(PARQUET_SCHEMA_SELECTOR, PARQUET_SCHEMA_INPUT_TEXT)
            .required(true)
            .build();

    public static final PropertyDescriptor SCHEMA_FILE_JSON = new PropertyDescriptor.Builder()
            .name("parquet-schema-file")
            .displayName("Parquet schema file location")
            .description("Location of a json file that describes the data structure of the parquet file")
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .dependsOn(PARQUET_SCHEMA_SELECTOR, PARQUET_SCHEMA_FILE)
            .required(true)
            .build();

    private DeltaLakeService deltalakeService;

    @Override
    protected void init(final ProcessorInitializationContext context) {
        this.relationships = Collections.unmodifiableSet(new HashSet<>(Arrays.asList(REL_SUCCESS, REL_FAILED)));
        this.descriptors = Collections.unmodifiableList(Arrays.asList(
                STORAGE_SELECTOR, INPUT_FILE_SELECTOR, LOCAL_PATH, PARQUET_SCHEMA_SELECTOR, SCHEMA_TEXT_JSON, SCHEMA_FILE_JSON,
                PARTITION_COLUMNS, INPUT_FILE_PATH_ATTRIBUTE, INPUT_FILE_NAME_ATTRIBUTE,
                S3_ACCESS_KEY, S3_SECRET_KEY, S3_BUCKET, S3_PATH,
                AZURE_ACCOUNT_KEY, AZURE_STORAGE_NAME, AZURE_STORAGE_ACCOUNT, AZURE_PATH,
                GCP_ACCOUNT_JSON_KEYFILE_PATH, GCP_BUCKET, GCP_PATH));
        deltalakeService = new DeltaLakeService();
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
    public void onTrigger(final ProcessContext processContext, final ProcessSession session) throws ProcessException {
        FlowFile file = session.get();
        FlowFile flowFile = file == null ? session.create() : file;
        try {
            deltalakeService.handleInputParquet(processContext, flowFile);
            Map<String, String> updateResult = updateDeltaLake();
            session.putAllAttributes(flowFile, updateResult);
            session.transfer(flowFile, REL_SUCCESS);
        } catch (Exception e) {
            session.putAttribute(flowFile, "delta table update failed with error", e.getMessage());
            session.transfer(flowFile, REL_FAILED);
            processContext.yield();
        }
    }

    @OnScheduled
    public void onScheduled(ProcessContext processContext) {
        deltalakeService.initialize(processContext);
    }

    private Map<String, String> updateDeltaLake() {
        Map<String, String> updateResult = new HashMap<>();

        if (deltalakeService.deltaTableExists()) {
            deltalakeService.startTransaction();
            int numberOfAddedFiles = deltalakeService.addNewFilesToDeltaTable();
            int numberOfRemovedFiles = deltalakeService.removeMissingFilesFromDeltaTable();
            deltalakeService.finishTransaction();

            updateResult.put("number of new files in the Delta table", String.valueOf(numberOfAddedFiles));
            updateResult.put("number of files removed from Delta table", String.valueOf(numberOfRemovedFiles));
        } else {
            int filesInNewDeltaTable = deltalakeService.createNewDeltaLakeTable();
            updateResult.put("number of files in the new DeltaTable", String.valueOf(filesInNewDeltaTable));
        }
        return updateResult;
    }

}

