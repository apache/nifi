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

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.nifi.annotation.behavior.SideEffectFree;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.flowfile.attributes.CoreAttributes;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processors.hadoop.util.SequenceFileWriter;
import org.apache.hadoop.io.SequenceFile.CompressionType;

/**
 * <p>
 * This processor is used to create a Hadoop Sequence File, which essentially is
 * a file of key/value pairs. The key will be a file name and the value will be
 * the flow file content. The processor will take either a merged (a.k.a.
 * packaged) flow file or a singular flow file. Historically, this processor
 * handled the merging by type and size or time prior to creating a SequenceFile
 * output; it no longer does this. If creating a SequenceFile that contains
 * multiple files of the same type is desired, precede this processor with a
 * <code>RouteOnAttribute</code> processor to segregate files of the same type
 * and follow that with a <code>MergeContent</code> processor to bundle up
 * files. If the type of files is not important, just use the
 * <code>MergeContent</code> processor. When using the <code>MergeContent</code>
 * processor, the following Merge Formats are supported by this processor:
 * <ul>
 * <li>TAR</li>
 * <li>ZIP</li>
 * <li>FlowFileStream v3</li>
 * </ul>
 * The created SequenceFile is named the same as the incoming FlowFile with the
 * suffix '.sf'. For incoming FlowFiles that are bundled, the keys in the
 * SequenceFile are the individual file names, the values are the contents of
 * each file.
 * </p>
 * NOTE: The value portion of a key/value pair is loaded into memory. While
 * there is a max size limit of 2GB, this could cause memory issues if there are
 * too many concurrent tasks and the flow file sizes are large.
 *
 */
@SideEffectFree
@Tags({"hadoop", "sequence file", "create", "sequencefile"})
@CapabilityDescription("Creates Hadoop Sequence Files from incoming flow files")
public class CreateHadoopSequenceFile extends AbstractHadoopProcessor {

    public static final String TAR_FORMAT = "tar";
    public static final String ZIP_FORMAT = "zip";
    public static final String FLOWFILE_STREAM_FORMAT_V3 = "flowfile-stream-v3";
    private static final String NOT_PACKAGED = "not packaged";

    // Relationships.
    public static final Relationship RELATIONSHIP_SUCCESS = new Relationship.Builder()
            .name("success")
            .description("Generated Sequence Files are sent to this relationship")
            .build();
    public static final Relationship RELATIONSHIP_FAILURE = new Relationship.Builder()
            .name("failure")
            .description("Incoming files that failed to generate a Sequence File are sent to this relationship")
            .build();
    private static final Set<Relationship> relationships;

    static {
        Set<Relationship> rels = new HashSet<>();
        rels.add(RELATIONSHIP_SUCCESS);
        rels.add(RELATIONSHIP_FAILURE);
        relationships = Collections.unmodifiableSet(rels);
    }
    // Optional Properties.
    static final PropertyDescriptor COMPRESSION_TYPE = new PropertyDescriptor.Builder()
            .name("compression type")
            .description("Type of compression to use when creating Sequence File")
            .allowableValues(CompressionType.values())
            .build();

    private static final List<PropertyDescriptor> props;

    static {
        List<PropertyDescriptor> someProps = new ArrayList<>(properties);
        someProps.add(COMPRESSION_TYPE);
        props = Collections.unmodifiableList(someProps);
    }
    // Default Values.
    public static final String DEFAULT_COMPRESSION_TYPE = "NONE";

    @Override
    public Set<Relationship> getRelationships() {
        return relationships;
    }

    @Override
    public List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return props;
    }

    @Override
    public void onTrigger(ProcessContext context, ProcessSession session) throws ProcessException {

        FlowFile flowFile = session.get();
        if (flowFile == null) {
            return;
        }

        String mimeType = flowFile.getAttribute(CoreAttributes.MIME_TYPE.key());
        String packagingFormat = NOT_PACKAGED;
        if (null != mimeType) {
            switch (mimeType.toLowerCase()) {
                case "application/tar":
                    packagingFormat = TAR_FORMAT;
                    break;
                case "application/zip":
                    packagingFormat = ZIP_FORMAT;
                    break;
                case "application/flowfile-v3":
                    packagingFormat = FLOWFILE_STREAM_FORMAT_V3;
                    break;
                default:
                    getLogger().warn(
                            "Cannot unpack {} because its mime.type attribute is set to '{}', which is not a format that can be unpacked",
                            new Object[]{flowFile, mimeType});
            }
        }
        final SequenceFileWriter sequenceFileWriter;
        switch (packagingFormat) {
            case TAR_FORMAT:
                sequenceFileWriter = new TarUnpackerSequenceFileWriter();
                break;
            case ZIP_FORMAT:
                sequenceFileWriter = new ZipUnpackerSequenceFileWriter();
                break;
            case FLOWFILE_STREAM_FORMAT_V3:
                sequenceFileWriter = new FlowFileStreamUnpackerSequenceFileWriter();
                break;
            default:
                sequenceFileWriter = new SequenceFileWriterImpl();
        }
        String value = context.getProperty(COMPRESSION_TYPE).getValue();
        CompressionType compressionType = value == null ? CompressionType.valueOf(DEFAULT_COMPRESSION_TYPE) : CompressionType.valueOf(value);
        final String fileName = flowFile.getAttribute(CoreAttributes.FILENAME.key()) + ".sf";
        flowFile = session.putAttribute(flowFile, CoreAttributes.FILENAME.key(), fileName);
        try {
            flowFile = sequenceFileWriter.writeSequenceFile(flowFile, session, hdfsResources.get().getKey(), compressionType);
            session.transfer(flowFile, RELATIONSHIP_SUCCESS);
            getLogger().info("Transferred flowfile {} to {}", new Object[]{flowFile, RELATIONSHIP_SUCCESS});
        } catch (Exception e) {
            getLogger().error("Failed to create Sequence File. Transferring {} to 'failure'", new Object[]{flowFile}, e);
            session.transfer(flowFile, RELATIONSHIP_FAILURE);
        }

    }
}
