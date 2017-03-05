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
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.nifi.annotation.behavior.EventDriven;
import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.SeeAlso;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.io.OutputStreamCallback;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.util.StopWatch;

import alluxio.AlluxioURI;
import alluxio.client.ReadType;
import alluxio.client.file.FileInStream;
import alluxio.client.file.URIStatus;
import alluxio.client.file.options.OpenFileOptions;
import alluxio.exception.FileDoesNotExistException;

@Tags({"alluxio", "tachyon", "get", "file", "fetch"})
@EventDriven
@InputRequirement(InputRequirement.Requirement.INPUT_REQUIRED)
@CapabilityDescription("!!EXPERIMENTAL!! User must be aware that due to some limitations, it is currently impossible to exchange "
        + "with multiple Alluxio clusters. Besides once Alluxio client has been initialized, modifications will only be taken into "
        + "account after NiFi is restarted (JVM restart). This processor will access the file using the input path provided and "
        + "write the content of the remote file to the content of the incoming FlowFile.")
@SeeAlso({ListAlluxio.class, PutAlluxio.class})
public class FetchAlluxio extends AbstractAlluxioProcessor {

    public static final PropertyDescriptor READ_TYPE = new PropertyDescriptor.Builder()
            .name("alluxio-read-type")
            .displayName("Read type")
            .description("The Read Type to use when accessing the remote file")
            .defaultValue(ReadType.CACHE_PROMOTE.toString())
            .required(true)
            .allowableValues(ReadType.values())
            .build();

    public static final PropertyDescriptor PATH = new PropertyDescriptor.Builder()
            .name("alluxio-path")
            .displayName("Path")
            .description("Path of the file to fetch from Alluxion")
            .required(true)
            .expressionLanguageSupported(true)
            .defaultValue("${alluxio_path}")
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    private final static List<PropertyDescriptor> propertyDescriptors;

    // Relationships
    public static final Relationship REL_SUCCESS = new Relationship.Builder()
            .name("success")
            .description("All files successfully retrieved are routed to this relationship.")
            .build();
    public static final Relationship REL_FAILURE = new Relationship.Builder()
            .name("failure")
            .description("In case of failure, FlowFiles will be routed to this relationship.")
            .autoTerminateDefault(true)
            .build();
    public static final Relationship REL_NOT_FOUND = new Relationship.Builder()
            .name("notfound")
            .description("Any FlowFile that could not be fetched from Alluxio because the file could not be found will be transferred to this relationship.")
            .autoTerminateDefault(true)
            .build();

    private final static Set<Relationship> relationships;

    static {
        List<PropertyDescriptor> _propertyDescriptors = new ArrayList<>();
        _propertyDescriptors.addAll(descriptors);
        _propertyDescriptors.add(PATH);
        _propertyDescriptors.add(READ_TYPE);
        propertyDescriptors = Collections.unmodifiableList(_propertyDescriptors);

        Set<Relationship> _relationships = new HashSet<>();
        _relationships.add(REL_SUCCESS);
        _relationships.add(REL_FAILURE);
        _relationships.add(REL_NOT_FOUND);
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
        final String uri = context.getProperty(PATH).evaluateAttributeExpressions(flowFile).getValue();

        if(StringUtils.isBlank(uri)) {
            getLogger().error("Cannot fetch a file without a path; transferring to 'failure'");
            session.transfer(flowFile, REL_FAILURE);
            return;
        }

        final AlluxioURI path = new AlluxioURI(uri);
        final OpenFileOptions options = OpenFileOptions.defaults().setReadType(ReadType.valueOf(context.getProperty(READ_TYPE).getValue()));

        FileInStream in = null;

        try {
            final URIStatus status = fileSystem.get().getStatus(path);
            flowFile = updateFlowFile(status, flowFile, session);

            in = fileSystem.get().openFile(path, options);
            final FileInStream toCopy = in;

            flowFile = session.write(flowFile, new OutputStreamCallback() {
                @Override
                public void process(final OutputStream out) throws IOException {
                    IOUtils.copy(toCopy, out);
                }
            });

            getLogger().info("Successfully retrieved {}; transferring to 'success'", new Object[]{uri});
            session.getProvenanceReporter().modifyContent(flowFile, "Replaced content of FlowFile with contents of " + uri, stopWatch.getElapsed(TimeUnit.MILLISECONDS));
            session.transfer(flowFile, REL_SUCCESS);
        } catch (FileDoesNotExistException e) {
            getLogger().error("The file {} does not exist; transferring to 'notfound'", new Object[]{uri}, e);
            session.transfer(flowFile, REL_NOT_FOUND);
        } catch (Exception e) {
            context.yield();
            getLogger().error("An error occurred while getting file {}; transferring to 'failure'", new Object[]{uri}, e);
            session.transfer(flowFile, REL_FAILURE);
        } finally {
            if(in != null) {
                try {
                    in.close();
                } catch (IOException e) {
                    getLogger().warn("Error while closing stream on {}.", new Object[]{uri}, e);
                }
            }
        }
    }

}