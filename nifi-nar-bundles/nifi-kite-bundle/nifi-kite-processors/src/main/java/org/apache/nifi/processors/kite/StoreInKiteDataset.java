/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.nifi.processors.kite;

import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import org.apache.avro.Schema;
import org.apache.avro.file.DataFileStream;
import org.apache.avro.generic.GenericData.Record;
import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.InputRequirement.Requirement;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.io.InputStreamCallback;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.util.StopWatch;
import org.kitesdk.data.DatasetIOException;
import org.kitesdk.data.DatasetWriter;
import org.kitesdk.data.Datasets;
import org.kitesdk.data.IncompatibleSchemaException;
import org.kitesdk.data.ValidationException;
import org.kitesdk.data.View;
import org.kitesdk.data.spi.SchemaValidationUtil;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;

@InputRequirement(Requirement.INPUT_REQUIRED)
@Tags({"kite", "avro", "parquet", "hadoop", "hive", "hdfs", "hbase"})
@CapabilityDescription("Stores Avro records in a Kite dataset")
public class StoreInKiteDataset extends AbstractKiteProcessor {

    private static final Relationship SUCCESS = new Relationship.Builder()
            .name("success")
            .description("FlowFile content has been successfully saved")
            .build();

    private static final Relationship INCOMPATIBLE = new Relationship.Builder()
            .name("incompatible")
            .description("FlowFile content is not compatible with the target dataset")
            .build();

    private static final Relationship FAILURE = new Relationship.Builder()
            .name("failure")
            .description("FlowFile content could not be processed")
            .build();

    public static final PropertyDescriptor KITE_DATASET_URI
            = new PropertyDescriptor.Builder()
            .name("Target dataset URI")
            .description("URI that identifies a Kite dataset where data will be stored")
            .addValidator(RECOGNIZED_URI)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .required(true)
            .build();

    public static final PropertyDescriptor ADDITIONAL_CLASSPATH_RESOURCES = new PropertyDescriptor.Builder()
            .name("additional-classpath-resources")
            .displayName("Additional Classpath Resources")
            .description("A comma-separated list of paths to files and/or directories that will be added to the classpath. When specifying a " +
                    "directory, all files with in the directory will be added to the classpath, but further sub-directories will not be included.")
            .required(false)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .dynamicallyModifiesClasspath(true)
            .build();

    private static final List<PropertyDescriptor> PROPERTIES
            = ImmutableList.<PropertyDescriptor>builder()
            .addAll(AbstractKiteProcessor.getProperties())
            .add(KITE_DATASET_URI)
            .add(ADDITIONAL_CLASSPATH_RESOURCES)
            .build();

    private static final Set<Relationship> RELATIONSHIPS
            = ImmutableSet.<Relationship>builder()
            .add(SUCCESS)
            .add(INCOMPATIBLE)
            .add(FAILURE)
            .build();

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return PROPERTIES;
    }

    @Override
    public Set<Relationship> getRelationships() {
        return RELATIONSHIPS;
    }

    @Override
    public void onTrigger(ProcessContext context, final ProcessSession session)
            throws ProcessException {
        FlowFile flowFile = session.get();
        if (flowFile == null) {
            return;
        }

        final View<Record> target = load(context, flowFile);
        final Schema schema = target.getDataset().getDescriptor().getSchema();

        try {
            StopWatch timer = new StopWatch(true);
            session.read(flowFile, new InputStreamCallback() {
                @Override
                public void process(InputStream in) throws IOException {
                    try (DataFileStream<Record> stream = new DataFileStream<>(
                            in, AvroUtil.newDatumReader(schema, Record.class))) {
                        IncompatibleSchemaException.check(
                                SchemaValidationUtil.canRead(stream.getSchema(), schema),
                                "Incompatible file schema %s, expected %s",
                                stream.getSchema(), schema);

                        long written = 0L;
                        try (DatasetWriter<Record> writer = target.newWriter()) {
                            for (Record record : stream) {
                                writer.write(record);
                                written += 1;
                            }
                        } finally {
                            session.adjustCounter("Stored records", written,
                                    true /* cannot roll back the write */);
                        }
                    }
                }
            });
            timer.stop();

            session.getProvenanceReporter().send(flowFile,
                    target.getUri().toString(),
                    timer.getDuration(TimeUnit.MILLISECONDS),
                    true /* cannot roll back the write */);

            session.transfer(flowFile, SUCCESS);

        } catch (ProcessException | DatasetIOException e) {
            getLogger().error("Failed to read FlowFile", e);
            session.transfer(flowFile, FAILURE);

        } catch (ValidationException e) {
            getLogger().error(e.getMessage());
            getLogger().debug("Incompatible schema error", e);
            session.transfer(flowFile, INCOMPATIBLE);
        }
    }

    private View<Record> load(ProcessContext context, FlowFile file) {
        String uri = context.getProperty(KITE_DATASET_URI)
                .evaluateAttributeExpressions(file)
                .getValue();
        return Datasets.load(uri, Record.class);
    }
}
