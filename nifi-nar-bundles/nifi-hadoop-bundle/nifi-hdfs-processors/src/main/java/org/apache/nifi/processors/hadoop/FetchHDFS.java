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
package org.apache.nifi.processors.hadoop;

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.hadoop.security.AccessControlException;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.InputRequirement.Requirement;
import org.apache.nifi.annotation.behavior.Restricted;
import org.apache.nifi.annotation.behavior.Restriction;
import org.apache.nifi.annotation.behavior.SupportsBatching;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.SeeAlso;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.RequiredPermission;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.flowfile.attributes.CoreAttributes;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.util.StopWatch;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.security.PrivilegedAction;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;

@SupportsBatching
@InputRequirement(Requirement.INPUT_REQUIRED)
@Tags({"hadoop", "hdfs", "get", "ingest", "fetch", "source"})
@CapabilityDescription("Retrieves a file from HDFS. The content of the incoming FlowFile is replaced by the content of the file in HDFS. "
        + "The file in HDFS is left intact without any changes being made to it.")
@WritesAttribute(attribute="hdfs.failure.reason", description="When a FlowFile is routed to 'failure', this attribute is added indicating why the file could "
        + "not be fetched from HDFS")
@SeeAlso({ListHDFS.class, GetHDFS.class, PutHDFS.class})
@Restricted(restrictions = {
    @Restriction(
        requiredPermission = RequiredPermission.READ_FILESYSTEM,
        explanation = "Provides operator the ability to retrieve any file that NiFi has access to in HDFS or the local filesystem.")
})
public class FetchHDFS extends AbstractHadoopProcessor {

    static final PropertyDescriptor FILENAME = new PropertyDescriptor.Builder()
        .name("HDFS Filename")
        .description("The name of the HDFS file to retrieve")
        .required(true)
        .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
        .defaultValue("${path}/${filename}")
        .addValidator(StandardValidators.ATTRIBUTE_EXPRESSION_LANGUAGE_VALIDATOR)
        .build();

    static final Relationship REL_SUCCESS = new Relationship.Builder()
        .name("success")
        .description("FlowFiles will be routed to this relationship once they have been updated with the content of the HDFS file")
        .build();
    static final Relationship REL_FAILURE = new Relationship.Builder()
        .name("failure")
        .description("FlowFiles will be routed to this relationship if the content of the HDFS file cannot be retrieved and trying again will likely not be helpful. "
                + "This would occur, for instance, if the file is not found or if there is a permissions issue")
        .build();
    static final Relationship REL_COMMS_FAILURE = new Relationship.Builder()
        .name("comms.failure")
        .description("FlowFiles will be routed to this relationship if the content of the HDFS file cannot be retrieve due to a communications failure. "
                + "This generally indicates that the Fetch should be tried again.")
        .build();

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        final List<PropertyDescriptor> props = new ArrayList<>(properties);
        props.add(FILENAME);
        props.add(COMPRESSION_CODEC);
        return props;
    }

    @Override
    public Set<Relationship> getRelationships() {
        final Set<Relationship> relationships = new HashSet<>();
        relationships.add(REL_SUCCESS);
        relationships.add(REL_FAILURE);
        relationships.add(REL_COMMS_FAILURE);
        return relationships;
    }

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {
        FlowFile flowFile = session.get();
        if ( flowFile == null ) {
            return;
        }

        final FileSystem hdfs = getFileSystem();
        final UserGroupInformation ugi = getUserGroupInformation();
        final String filenameValue = context.getProperty(FILENAME).evaluateAttributeExpressions(flowFile).getValue();

        final Path path;
        try {
            path = new Path(filenameValue);
        } catch (IllegalArgumentException e) {
            getLogger().error("Failed to retrieve content from {} for {} due to {}; routing to failure", new Object[] {filenameValue, flowFile, e});
            flowFile = session.putAttribute(flowFile, "hdfs.failure.reason", e.getMessage());
            flowFile = session.penalize(flowFile);
            session.transfer(flowFile, REL_FAILURE);
            return;
        }

        final StopWatch stopWatch = new StopWatch(true);
        final FlowFile finalFlowFile = flowFile;

        ugi.doAs(new PrivilegedAction<Object>() {
            @Override
            public Object run() {
                InputStream stream = null;
                CompressionCodec codec = null;
                Configuration conf = getConfiguration();
                final CompressionCodecFactory compressionCodecFactory = new CompressionCodecFactory(conf);
                final CompressionType compressionType = CompressionType.valueOf(context.getProperty(COMPRESSION_CODEC).toString());
                final boolean inferCompressionCodec = compressionType == CompressionType.AUTOMATIC;

                if(inferCompressionCodec) {
                    codec = compressionCodecFactory.getCodec(path);
                } else if (compressionType != CompressionType.NONE) {
                    codec = getCompressionCodec(context, getConfiguration());
                }

                FlowFile flowFile = finalFlowFile;
                final Path qualifiedPath = path.makeQualified(hdfs.getUri(), hdfs.getWorkingDirectory());
                try {
                    final String outputFilename;
                    final String originalFilename = path.getName();
                    stream = hdfs.open(path, 16384);

                    // Check if compression codec is defined (inferred or otherwise)
                    if (codec != null) {
                        stream = codec.createInputStream(stream);
                        outputFilename = StringUtils.removeEnd(originalFilename, codec.getDefaultExtension());
                    } else {
                        outputFilename = originalFilename;
                    }

                    flowFile = session.importFrom(stream, finalFlowFile);
                    flowFile = session.putAttribute(flowFile, CoreAttributes.FILENAME.key(), outputFilename);

                    stopWatch.stop();
                    getLogger().info("Successfully received content from {} for {} in {}", new Object[] {qualifiedPath, flowFile, stopWatch.getDuration()});
                    session.getProvenanceReporter().fetch(flowFile, qualifiedPath.toString(), stopWatch.getDuration(TimeUnit.MILLISECONDS));
                    session.transfer(flowFile, REL_SUCCESS);
                } catch (final FileNotFoundException | AccessControlException e) {
                    getLogger().error("Failed to retrieve content from {} for {} due to {}; routing to failure", new Object[] {qualifiedPath, flowFile, e});
                    flowFile = session.putAttribute(flowFile, "hdfs.failure.reason", e.getMessage());
                    flowFile = session.penalize(flowFile);
                    session.transfer(flowFile, REL_FAILURE);
                } catch (final IOException e) {
                    getLogger().error("Failed to retrieve content from {} for {} due to {}; routing to comms.failure", new Object[] {qualifiedPath, flowFile, e});
                    flowFile = session.penalize(flowFile);
                    session.transfer(flowFile, REL_COMMS_FAILURE);
                } finally {
                    IOUtils.closeQuietly(stream);
                }

                return null;
            }
        });

    }

}
