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
package org.apache.nifi.processors.alluxio;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import org.apache.commons.io.IOUtils;
import org.apache.nifi.annotation.behavior.EventDriven;
import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.SeeAlso;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.DataUnit;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.io.InputStreamCallback;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.util.StopWatch;

import alluxio.AlluxioURI;
import alluxio.Constants;
import alluxio.client.WriteType;
import alluxio.client.file.FileOutStream;
import alluxio.client.file.options.CreateFileOptions;

@Tags({"alluxio", "tachyon", "put", "file"})
@EventDriven
@InputRequirement(InputRequirement.Requirement.INPUT_REQUIRED)
@CapabilityDescription("!!EXPERIMENTAL!! User must be aware that due to some limitations, it is currently impossible to exchange "
        + "with multiple Alluxio clusters. Besides once Alluxio client has been initialized, modifications will only be taken into "
        + "account after NiFi is restarted (JVM restart). This processor will write incoming flow files to the given URI.")
@SeeAlso({FetchAlluxio.class, ListAlluxio.class})
public class PutAlluxio extends AbstractAlluxioProcessor {

    public static final PropertyDescriptor URI = new PropertyDescriptor.Builder()
            .name("alluxio-uri")
            .displayName("URI")
            .description("Target path (including file name) where the file should be written to. Example: /path/my-file.txt")
            .required(true)
            .expressionLanguageSupported(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final PropertyDescriptor WRITE_TYPE = new PropertyDescriptor.Builder()
            .name("alluxio-write-type")
            .displayName("Write type")
            .description("The Write Type to use when writing the remote file")
            .defaultValue(WriteType.MUST_CACHE.toString())
            .required(true)
            .allowableValues(WriteType.values())
            .build();

    public static final PropertyDescriptor TTL = new PropertyDescriptor.Builder()
            .name("alluxio-ttl")
            .displayName("Time to live")
            .description("TTL identifies duration (in milliseconds) the created file should be kept around before it is automatically deleted. " +
                    "By default (-1), the file will not be deleted.")
            .defaultValue(String.valueOf(Constants.NO_TTL))
            .required(true)
            .addValidator(StandardValidators.LONG_VALIDATOR)
            .build();

    public static final PropertyDescriptor BLOCK_SIZE = new PropertyDescriptor.Builder()
            .name("alluxio-block-size")
            .displayName("Block size")
            .description("Block size that will be used to write the file. If not set, the default configured in Alluxio will be used.")
            .required(false)
            .addValidator(StandardValidators.DATA_SIZE_VALIDATOR)
            .build();

    private final static List<PropertyDescriptor> propertyDescriptors;

    // Relationships
    public static final Relationship REL_SUCCESS = new Relationship.Builder()
            .name("success")
            .description("All files successfully written are routed to this relationship")
            .build();
    public static final Relationship REL_FAILURE = new Relationship.Builder()
            .name("failure")
            .description("In case of failure, FlowFiles will be routed to this relationship")
            .build();

    private final static Set<Relationship> relationships;

    /*
     * Will ensure that the list of property descriptors is build only once.
     * Will also create a Set of relationships
     */
    static {
        List<PropertyDescriptor> _propertyDescriptors = new ArrayList<>();
        _propertyDescriptors.addAll(descriptors);
        _propertyDescriptors.add(URI);
        _propertyDescriptors.add(WRITE_TYPE);
        _propertyDescriptors.add(TTL);
        _propertyDescriptors.add(BLOCK_SIZE);
        propertyDescriptors = Collections.unmodifiableList(_propertyDescriptors);

        Set<Relationship> _relationships = new HashSet<>();
        _relationships.add(REL_SUCCESS);
        _relationships.add(REL_FAILURE);
        relationships = Collections.unmodifiableSet(_relationships);
    }

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return propertyDescriptors;
    }

    @Override
    public Set<Relationship> getRelationships() {
        return relationships;
    }

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {
        FlowFile flowFile = session.get();
        if (flowFile == null) {
            return;
        }

        final StopWatch stopWatch = new StopWatch(true);
        final String uri = context.getProperty(URI).evaluateAttributeExpressions(flowFile).getValue();
        final AlluxioURI path = new AlluxioURI(uri);
        final CreateFileOptions options = CreateFileOptions.defaults();
        final Double blockSize = context.getProperty(BLOCK_SIZE).asDataSize(DataUnit.B);

        if(blockSize != null) {
            options.setBlockSizeBytes(blockSize.longValue());
        }
        options.setTtl(context.getProperty(TTL).asLong());
        options.setWriteType(WriteType.valueOf(context.getProperty(WRITE_TYPE).getValue()));

        FileOutStream out = null;

        try {
            out = fileSystem.get().createFile(path, options);
            final FileOutStream toWrite = out;
            session.read(flowFile, new InputStreamCallback() {
                @Override
                public void process(final InputStream in) throws IOException {
                    IOUtils.copy(in, toWrite);
                }
            });

            getLogger().info("Successfully wrote {}; transferring to 'success'", new Object[]{uri});
            session.getProvenanceReporter().receive(flowFile, uri, stopWatch.getElapsed(TimeUnit.MILLISECONDS));
            session.transfer(flowFile, REL_SUCCESS);
        } catch (Exception e) {
            context.yield();
            getLogger().error("An error occurred while writing file {}; transferring to 'failure'", new Object[]{uri}, e);
            session.transfer(flowFile, REL_FAILURE);
        } finally {
            if(out != null) {
                try {
                    out.close();
                } catch (IOException e) {
                    getLogger().warn("Error while closing stream on {}.", new Object[]{uri}, e);
                }
            }
        }
    }

}