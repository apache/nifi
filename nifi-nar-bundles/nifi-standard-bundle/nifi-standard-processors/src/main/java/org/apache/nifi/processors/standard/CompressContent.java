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
package org.apache.nifi.processors.standard;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import lzma.sdk.lzma.Decoder;
import lzma.streams.LzmaInputStream;
import lzma.streams.LzmaOutputStream;
import org.apache.commons.compress.compressors.CompressorStreamFactory;
import org.apache.commons.compress.compressors.bzip2.BZip2CompressorInputStream;
import org.apache.commons.compress.compressors.gzip.GzipCompressorInputStream;
import org.apache.nifi.annotation.behavior.EventDriven;
import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.InputRequirement.Requirement;
import org.apache.nifi.annotation.behavior.ReadsAttribute;
import org.apache.nifi.annotation.behavior.SideEffectFree;
import org.apache.nifi.annotation.behavior.SupportsBatching;
import org.apache.nifi.annotation.behavior.SystemResource;
import org.apache.nifi.annotation.behavior.SystemResourceConsideration;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.flowfile.attributes.CoreAttributes;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.ProcessorInitializationContext;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.io.StreamCallback;
import org.apache.nifi.stream.io.GZIPOutputStream;
import org.apache.nifi.util.StopWatch;
import org.tukaani.xz.LZMA2Options;
import org.tukaani.xz.XZInputStream;
import org.tukaani.xz.XZOutputStream;
import org.xerial.snappy.SnappyFramedInputStream;
import org.xerial.snappy.SnappyFramedOutputStream;
import org.xerial.snappy.SnappyInputStream;
import org.xerial.snappy.SnappyOutputStream;

@EventDriven
@SideEffectFree
@SupportsBatching
@InputRequirement(Requirement.INPUT_REQUIRED)
@Tags({"content", "compress", "decompress", "gzip", "bzip2", "lzma", "xz-lzma2", "snappy", "snappy framed"})
@CapabilityDescription("Compresses or decompresses the contents of FlowFiles using a user-specified compression algorithm and updates the mime.type "
    + "attribute as appropriate")
@ReadsAttribute(attribute = "mime.type", description = "If the Compression Format is set to use mime.type attribute, this attribute is used to "
    + "determine the compression type. Otherwise, this attribute is ignored.")
@WritesAttribute(attribute = "mime.type", description = "If the Mode property is set to compress, the appropriate MIME Type is set. If the Mode "
    + "property is set to decompress and the file is successfully decompressed, this attribute is removed, as the MIME Type is no longer known.")
@SystemResourceConsideration(resource = SystemResource.CPU)
public class CompressContent extends AbstractProcessor {

    public static final String COMPRESSION_FORMAT_ATTRIBUTE = "use mime.type attribute";
    public static final String COMPRESSION_FORMAT_GZIP = "gzip";
    public static final String COMPRESSION_FORMAT_BZIP2 = "bzip2";
    public static final String COMPRESSION_FORMAT_XZ_LZMA2 = "xz-lzma2";
    public static final String COMPRESSION_FORMAT_LZMA = "lzma";
    public static final String COMPRESSION_FORMAT_SNAPPY = "snappy";
    public static final String COMPRESSION_FORMAT_SNAPPY_FRAMED = "snappy framed";

    public static final String MODE_COMPRESS = "compress";
    public static final String MODE_DECOMPRESS = "decompress";

    public static final PropertyDescriptor COMPRESSION_FORMAT = new PropertyDescriptor.Builder()
    .name("Compression Format")
    .description("The compression format to use. Valid values are: GZIP, BZIP2, XZ-LZMA2, LZMA, Snappy, and Snappy Framed")
    .allowableValues(COMPRESSION_FORMAT_ATTRIBUTE, COMPRESSION_FORMAT_GZIP, COMPRESSION_FORMAT_BZIP2,
            COMPRESSION_FORMAT_XZ_LZMA2, COMPRESSION_FORMAT_LZMA, COMPRESSION_FORMAT_SNAPPY, COMPRESSION_FORMAT_SNAPPY_FRAMED)
    .defaultValue(COMPRESSION_FORMAT_ATTRIBUTE)
    .required(true)
    .build();
    public static final PropertyDescriptor COMPRESSION_LEVEL = new PropertyDescriptor.Builder()
    .name("Compression Level")
    .description("The compression level to use; this is valid only when using GZIP compression. A lower value results in faster processing "
        + "but less compression; a value of 0 indicates no compression but simply archiving")
        .defaultValue("1")
        .required(true)
        .allowableValues("0", "1", "2", "3", "4", "5", "6", "7", "8", "9")
        .build();
    public static final PropertyDescriptor MODE = new PropertyDescriptor.Builder()
    .name("Mode")
    .description("Indicates whether the processor should compress content or decompress content. Must be either 'compress' or 'decompress'")
    .allowableValues(MODE_COMPRESS, MODE_DECOMPRESS)
    .defaultValue(MODE_COMPRESS)
    .required(true)
    .build();
    public static final PropertyDescriptor UPDATE_FILENAME = new PropertyDescriptor.Builder()
    .name("Update Filename")
    .description("If true, will remove the filename extension when decompressing data (only if the extension indicates the appropriate "
        + "compression format) and add the appropriate extension when compressing data")
        .required(true)
        .allowableValues("true", "false")
        .defaultValue("false")
        .build();

    public static final Relationship REL_SUCCESS = new Relationship.Builder()
    .name("success")
    .description("FlowFiles will be transferred to the success relationship after successfully being compressed or decompressed")
    .build();
    public static final Relationship REL_FAILURE = new Relationship.Builder()
    .name("failure")
    .description("FlowFiles will be transferred to the failure relationship if they fail to compress/decompress")
    .build();

    private List<PropertyDescriptor> properties;
    private Set<Relationship> relationships;
    private Map<String, String> compressionFormatMimeTypeMap;

    @Override
    protected void init(final ProcessorInitializationContext context) {
        final List<PropertyDescriptor> properties = new ArrayList<>();
        properties.add(MODE);
        properties.add(COMPRESSION_FORMAT);
        properties.add(COMPRESSION_LEVEL);
        properties.add(UPDATE_FILENAME);
        this.properties = Collections.unmodifiableList(properties);

        final Set<Relationship> relationships = new HashSet<>();
        relationships.add(REL_SUCCESS);
        relationships.add(REL_FAILURE);
        this.relationships = Collections.unmodifiableSet(relationships);

        final Map<String, String> mimeTypeMap = new HashMap<>();
        mimeTypeMap.put("application/gzip", COMPRESSION_FORMAT_GZIP);
        mimeTypeMap.put("application/x-gzip", COMPRESSION_FORMAT_GZIP);
        mimeTypeMap.put("application/bzip2", COMPRESSION_FORMAT_BZIP2);
        mimeTypeMap.put("application/x-bzip2", COMPRESSION_FORMAT_BZIP2);
        mimeTypeMap.put("application/x-lzma", COMPRESSION_FORMAT_LZMA);
        mimeTypeMap.put("application/x-snappy", COMPRESSION_FORMAT_SNAPPY);
        mimeTypeMap.put("application/x-snappy-framed", COMPRESSION_FORMAT_SNAPPY_FRAMED);
        this.compressionFormatMimeTypeMap = Collections.unmodifiableMap(mimeTypeMap);
    }

    @Override
    public Set<Relationship> getRelationships() {
        return relationships;
    }

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return properties;
    }

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) {
        FlowFile flowFile = session.get();
        if (flowFile == null) {
            return;
        }

        final ComponentLog logger = getLogger();
        final long sizeBeforeCompression = flowFile.getSize();
        final String compressionMode = context.getProperty(MODE).getValue();

        String compressionFormatValue = context.getProperty(COMPRESSION_FORMAT).getValue();
        if (compressionFormatValue.equals(COMPRESSION_FORMAT_ATTRIBUTE)) {
            final String mimeType = flowFile.getAttribute(CoreAttributes.MIME_TYPE.key());
            if (mimeType == null) {
                logger.error("No {} attribute exists for {}; routing to failure", new Object[]{CoreAttributes.MIME_TYPE.key(), flowFile});
                session.transfer(flowFile, REL_FAILURE);
                return;
            }

            compressionFormatValue = compressionFormatMimeTypeMap.get(mimeType);
            if (compressionFormatValue == null) {
                logger.info("Mime Type of {} is '{}', which does not indicate a supported Compression Format; routing to success without decompressing",
                    new Object[]{flowFile, mimeType});
                session.transfer(flowFile, REL_SUCCESS);
                return;
            }
        }

        final String compressionFormat = compressionFormatValue;
        final AtomicReference<String> mimeTypeRef = new AtomicReference<>(null);
        final StopWatch stopWatch = new StopWatch(true);

        final String fileExtension;
        switch (compressionFormat.toLowerCase()) {
            case COMPRESSION_FORMAT_GZIP:
                fileExtension = ".gz";
                break;
            case COMPRESSION_FORMAT_LZMA:
                fileExtension = ".lzma";
                break;
            case COMPRESSION_FORMAT_XZ_LZMA2:
                fileExtension = ".xz";
                break;
            case COMPRESSION_FORMAT_BZIP2:
                fileExtension = ".bz2";
                break;
            case COMPRESSION_FORMAT_SNAPPY:
                fileExtension = ".snappy";
                break;
            case COMPRESSION_FORMAT_SNAPPY_FRAMED:
                fileExtension = ".sz";
                break;
            default:
                fileExtension = "";
                break;
        }

        try {
            flowFile = session.write(flowFile, new StreamCallback() {
                @Override
                public void process(final InputStream rawIn, final OutputStream rawOut) throws IOException {
                    final OutputStream compressionOut;
                    final InputStream compressionIn;

                    final OutputStream bufferedOut = new BufferedOutputStream(rawOut, 65536);
                    final InputStream bufferedIn = new BufferedInputStream(rawIn, 65536);

                    try {
                        if (MODE_COMPRESS.equalsIgnoreCase(compressionMode)) {
                            compressionIn = bufferedIn;

                            switch (compressionFormat.toLowerCase()) {
                                case COMPRESSION_FORMAT_GZIP:
                                    final int compressionLevel = context.getProperty(COMPRESSION_LEVEL).asInteger();
                                    compressionOut = new GZIPOutputStream(bufferedOut, compressionLevel);
                                    mimeTypeRef.set("application/gzip");
                                    break;
                                case COMPRESSION_FORMAT_LZMA:
                                    compressionOut = new LzmaOutputStream.Builder(bufferedOut).build();
                                    mimeTypeRef.set("application/x-lzma");
                                    break;
                                case COMPRESSION_FORMAT_XZ_LZMA2:
                                    compressionOut = new XZOutputStream(bufferedOut, new LZMA2Options());
                                    mimeTypeRef.set("application/x-xz");
                                    break;
                                case COMPRESSION_FORMAT_SNAPPY:
                                    compressionOut = new SnappyOutputStream(bufferedOut);
                                    mimeTypeRef.set("application/x-snappy");
                                    break;
                                case COMPRESSION_FORMAT_SNAPPY_FRAMED:
                                    compressionOut = new SnappyFramedOutputStream(bufferedOut);
                                    mimeTypeRef.set("application/x-snappy-framed");
                                    break;
                                case COMPRESSION_FORMAT_BZIP2:
                                default:
                                    mimeTypeRef.set("application/x-bzip2");
                                    compressionOut = new CompressorStreamFactory().createCompressorOutputStream(compressionFormat.toLowerCase(), bufferedOut);
                                    break;
                            }
                        } else {
                            compressionOut = bufferedOut;
                            switch (compressionFormat.toLowerCase()) {
                                case COMPRESSION_FORMAT_LZMA:
                                    compressionIn = new LzmaInputStream(bufferedIn, new Decoder());
                                    break;
                                case COMPRESSION_FORMAT_XZ_LZMA2:
                                    compressionIn = new XZInputStream(bufferedIn);
                                    break;
                                case COMPRESSION_FORMAT_BZIP2:
                                    // need this two-arg constructor to support concatenated streams
                                    compressionIn = new BZip2CompressorInputStream(bufferedIn, true);
                                    break;
                                case COMPRESSION_FORMAT_GZIP:
                                    compressionIn = new GzipCompressorInputStream(bufferedIn, true);
                                    break;
                                case COMPRESSION_FORMAT_SNAPPY:
                                    compressionIn = new SnappyInputStream(bufferedIn);
                                    break;
                                case COMPRESSION_FORMAT_SNAPPY_FRAMED:
                                    compressionIn = new SnappyFramedInputStream(bufferedIn);
                                    break;
                                default:
                                    compressionIn = new CompressorStreamFactory().createCompressorInputStream(compressionFormat.toLowerCase(), bufferedIn);
                            }
                        }
                    } catch (final Exception e) {
                        closeQuietly(bufferedOut);
                        throw new IOException(e);
                    }

                    try (final InputStream in = compressionIn;
                        final OutputStream out = compressionOut) {
                        final byte[] buffer = new byte[8192];
                        int len;
                        while ((len = in.read(buffer)) > 0) {
                            out.write(buffer, 0, len);
                        }
                        out.flush();
                    }
                }
            });
            stopWatch.stop();

            final long sizeAfterCompression = flowFile.getSize();
            if (MODE_DECOMPRESS.equalsIgnoreCase(compressionMode)) {
                flowFile = session.removeAttribute(flowFile, CoreAttributes.MIME_TYPE.key());

                if (context.getProperty(UPDATE_FILENAME).asBoolean()) {
                    final String filename = flowFile.getAttribute(CoreAttributes.FILENAME.key());
                    if (filename.toLowerCase().endsWith(fileExtension)) {
                        flowFile = session.putAttribute(flowFile, CoreAttributes.FILENAME.key(), filename.substring(0, filename.length() - fileExtension.length()));
                    }
                }
            } else {
                flowFile = session.putAttribute(flowFile, CoreAttributes.MIME_TYPE.key(), mimeTypeRef.get());

                if (context.getProperty(UPDATE_FILENAME).asBoolean()) {
                    final String filename = flowFile.getAttribute(CoreAttributes.FILENAME.key());
                    flowFile = session.putAttribute(flowFile, CoreAttributes.FILENAME.key(), filename + fileExtension);
                }
            }

            logger.info("Successfully {}ed {} using {} compression format; size changed from {} to {} bytes",
                new Object[]{compressionMode.toLowerCase(), flowFile, compressionFormat, sizeBeforeCompression, sizeAfterCompression});
            session.getProvenanceReporter().modifyContent(flowFile, stopWatch.getDuration(TimeUnit.MILLISECONDS));
            session.transfer(flowFile, REL_SUCCESS);
        } catch (final ProcessException e) {
            logger.error("Unable to {} {} using {} compression format due to {}; routing to failure", new Object[]{compressionMode.toLowerCase(), flowFile, compressionFormat, e});
            session.transfer(flowFile, REL_FAILURE);
        }
    }

    private void closeQuietly(final Closeable closeable) {
        if (closeable != null) {
            try {
                closeable.close();
            } catch (final Exception e) {
            }
        }
    }
}
