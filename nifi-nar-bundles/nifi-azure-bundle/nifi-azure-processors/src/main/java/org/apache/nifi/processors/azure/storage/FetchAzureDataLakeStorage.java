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
package org.apache.nifi.processors.azure.storage;

import com.azure.core.util.Context;
import com.azure.storage.file.datalake.DataLakeDirectoryClient;
import com.azure.storage.file.datalake.DataLakeFileClient;
import com.azure.storage.file.datalake.DataLakeFileSystemClient;
import com.azure.storage.file.datalake.DataLakeServiceClient;
import com.azure.storage.file.datalake.models.DownloadRetryOptions;
import com.azure.storage.file.datalake.models.FileRange;
import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.InputRequirement.Requirement;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.SeeAlso;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.DataUnit;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.processors.azure.AbstractAzureDataLakeStorageProcessor;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.Stream;

@Tags({"azure", "microsoft", "cloud", "storage", "adlsgen2", "datalake"})
@SeeAlso({PutAzureDataLakeStorage.class, DeleteAzureDataLakeStorage.class, ListAzureDataLakeStorage.class})
@CapabilityDescription("Fetch the provided file from Azure Data Lake Storage")
@InputRequirement(Requirement.INPUT_REQUIRED)
public class FetchAzureDataLakeStorage extends AbstractAzureDataLakeStorageProcessor {

    public static final PropertyDescriptor RANGE_START = new PropertyDescriptor.Builder()
            .name("range-start")
            .displayName("Range Start")
            .description("The byte position at which to start reading from the object. An empty value or a value of " +
                    "zero will start reading at the beginning of the object.")
            .addValidator(StandardValidators.DATA_SIZE_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .required(false)
            .build();

    public static final PropertyDescriptor RANGE_LENGTH = new PropertyDescriptor.Builder()
            .name("range-length")
            .displayName("Range Length")
            .description("The number of bytes to download from the object, starting from the Range Start. An empty " +
                    "value or a value that extends beyond the end of the object will read to the end of the object.")
            .addValidator(StandardValidators.createDataSizeBoundsValidator(1, Long.MAX_VALUE))
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .required(false)
            .build();

    public static final PropertyDescriptor NUM_RETRIES = new PropertyDescriptor.Builder()
            .name("number-of-retries")
            .displayName("Number of Retries")
            .description("The number of automatic retries to perform if the download fails.")
            .addValidator(StandardValidators.createLongValidator(0L, Integer.MAX_VALUE, true))
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .required(false)
            .defaultValue("0")
            .build();

    private static final List<PropertyDescriptor> PROPERTIES = Collections.unmodifiableList(Arrays.asList(
            RANGE_START,
            RANGE_LENGTH,
            NUM_RETRIES
    ));

    @Override
    public List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return Stream.of(super.getSupportedPropertyDescriptors(), PROPERTIES)
                .flatMap(Collection::stream)
                .collect(Collectors.collectingAndThen(Collectors.toList(), Collections::unmodifiableList));
    }

    @Override
    public void onTrigger(ProcessContext context, ProcessSession session) throws ProcessException {
        FlowFile flowFile = session.get();
        if (flowFile == null) {
            return;
        }

        final long startNanos = System.nanoTime();
        try {
            final long rangeStart = (context.getProperty(RANGE_START).isSet() ? context.getProperty(RANGE_START).evaluateAttributeExpressions(flowFile).asDataSize(DataUnit.B).longValue() : 0L);
            final Long rangeLength = (context.getProperty(RANGE_LENGTH).isSet() ? context.getProperty(RANGE_LENGTH).evaluateAttributeExpressions(flowFile).asDataSize(DataUnit.B).longValue() : null);
            final int numRetries = (context.getProperty(NUM_RETRIES).isSet() ? context.getProperty(NUM_RETRIES).evaluateAttributeExpressions(flowFile).asInteger() : 0);
            final FileRange fileRange = new FileRange(rangeStart, rangeLength);
            final DownloadRetryOptions retryOptions = new DownloadRetryOptions();
            retryOptions.setMaxRetryRequests(numRetries);

            final String fileSystem = evaluateFileSystemProperty(context, flowFile);
            final String directory = evaluateDirectoryProperty(context, flowFile);
            final String fileName = evaluateFileNameProperty(context, flowFile);
            final DataLakeServiceClient storageClient = getStorageClient(context, flowFile);
            final DataLakeFileSystemClient fileSystemClient = storageClient.getFileSystemClient(fileSystem);
            final DataLakeDirectoryClient directoryClient = fileSystemClient.getDirectoryClient(directory);
            final DataLakeFileClient fileClient = directoryClient.getFileClient(fileName);

            if (fileClient.getProperties().isDirectory()) {
                throw new ProcessException(FILE.getDisplayName() + " (" + fileName + ") points to a directory. Full path: " + fileClient.getFilePath());
            }

            flowFile = session.write(flowFile, os -> fileClient.readWithResponse(os, fileRange, retryOptions, null, false, null, Context.NONE));
            session.getProvenanceReporter().modifyContent(flowFile);
            session.transfer(flowFile, REL_SUCCESS);

            final long transferMillis = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startNanos);
            session.getProvenanceReporter().fetch(flowFile, fileClient.getFileUrl(), transferMillis);
        } catch (Exception e) {
            getLogger().error("Failure to fetch file from Azure Data Lake Storage", e);
            flowFile = session.penalize(flowFile);
            session.transfer(flowFile, REL_FAILURE);
        }
    }
}
