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
package org.apache.nifi.processors.compress;

import com.aayushatharva.brotli4j.Brotli4jLoader;
import com.aayushatharva.brotli4j.decoder.BrotliInputStream;
import com.aayushatharva.brotli4j.encoder.BrotliOutputStream;
import com.aayushatharva.brotli4j.encoder.Encoder;
import lzma.sdk.lzma.Decoder;
import lzma.streams.LzmaInputStream;
import lzma.streams.LzmaOutputStream;
import org.apache.commons.compress.compressors.CompressorException;
import org.apache.commons.compress.compressors.CompressorStreamFactory;
import org.apache.commons.compress.compressors.bzip2.BZip2CompressorInputStream;
import org.apache.commons.compress.compressors.gzip.GzipCompressorInputStream;
import org.apache.commons.compress.compressors.lz4.FramedLZ4CompressorInputStream;
import org.apache.commons.compress.compressors.zstandard.ZstdCompressorInputStream;
import org.apache.commons.compress.compressors.zstandard.ZstdCompressorOutputStream;
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
import org.apache.nifi.processors.compress.util.CompressionInfo;
import org.apache.nifi.stream.io.GZIPOutputStream;
import org.apache.nifi.stream.io.StreamUtils;
import org.apache.nifi.util.StopWatch;
import org.apache.nifi.util.StringUtils;
import org.tukaani.xz.LZMA2Options;
import org.tukaani.xz.XZInputStream;
import org.tukaani.xz.XZOutputStream;
import org.xerial.snappy.SnappyFramedInputStream;
import org.xerial.snappy.SnappyFramedOutputStream;
import org.xerial.snappy.SnappyHadoopCompatibleOutputStream;
import org.xerial.snappy.SnappyInputStream;
import org.xerial.snappy.SnappyOutputStream;

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
import java.util.zip.Deflater;
import java.util.zip.DeflaterOutputStream;
import java.util.zip.InflaterInputStream;

@SideEffectFree
@SupportsBatching
@InputRequirement(Requirement.INPUT_REQUIRED)
@Tags({"content", "compress", "recompress", "gzip", "bzip2", "lzma", "xz-lzma2", "snappy", "snappy-hadoop", "snappy framed", "lz4-framed", "deflate", "zstd", "brotli"})
@CapabilityDescription("Decompresses the contents of FlowFiles using a user-specified compression algorithm and recompresses the contents using the specified compression format properties. "
        + "Also updates the mime.type attribute as appropriate. This processor operates in a very memory efficient way so very large objects well beyond the heap size "
        + "are generally fine to process")
@ReadsAttribute(attribute = "mime.type", description = "If the Decompression Format is set to 'use mime.type attribute', this attribute is used to "
        + "determine the decompression type. Otherwise, this attribute is ignored.")
@WritesAttribute(attribute = "mime.type", description = "The appropriate MIME Type is set based on the value of the Compression Format property. If the Compression Format is 'no compression' this "
        + "attribute is removed as the MIME Type is no longer known.")
@SystemResourceConsideration(resource = SystemResource.CPU)
@SystemResourceConsideration(resource = SystemResource.MEMORY)
public class ModifyCompression extends AbstractProcessor {

    private final static int STREAM_BUFFER_SIZE = 65536;

    public static final PropertyDescriptor INPUT_COMPRESSION = new PropertyDescriptor.Builder()
            .name("input-compression-format")
            .displayName("Input Compression Format")
            .description("The format to use for decompressing input FlowFiles.")
            .allowableValues(CompressionInfo.DECOMPRESSION_FORMAT_NONE.asAllowableValue(),
                    CompressionInfo.COMPRESSION_FORMAT_ATTRIBUTE.asAllowableValue(),
                    CompressionInfo.COMPRESSION_FORMAT_GZIP.asAllowableValue(),
                    CompressionInfo.COMPRESSION_FORMAT_DEFLATE.asAllowableValue(),
                    CompressionInfo.COMPRESSION_FORMAT_BZIP2.asAllowableValue(),
                    CompressionInfo.COMPRESSION_FORMAT_XZ_LZMA2.asAllowableValue(),
                    CompressionInfo.COMPRESSION_FORMAT_LZMA.asAllowableValue(),
                    CompressionInfo.COMPRESSION_FORMAT_SNAPPY.asAllowableValue(),
                    CompressionInfo.COMPRESSION_FORMAT_SNAPPY_FRAMED.asAllowableValue(),
                    CompressionInfo.COMPRESSION_FORMAT_LZ4_FRAMED.asAllowableValue(),
                    CompressionInfo.COMPRESSION_FORMAT_ZSTD.asAllowableValue(),
                    CompressionInfo.COMPRESSION_FORMAT_BROTLI.asAllowableValue())
            .defaultValue(CompressionInfo.DECOMPRESSION_FORMAT_NONE.getValue())
            .required(true)
            .build();
    public static final PropertyDescriptor OUTPUT_COMPRESSION = new PropertyDescriptor.Builder()
            .name("output-compression-format")
            .name("Output Compression Format")
            .description("The format to use for compressing output FlowFiles.")
            .allowableValues(CompressionInfo.COMPRESSION_FORMAT_NONE.asAllowableValue(),
                    CompressionInfo.COMPRESSION_FORMAT_GZIP.asAllowableValue(),
                    CompressionInfo.COMPRESSION_FORMAT_DEFLATE.asAllowableValue(),
                    CompressionInfo.COMPRESSION_FORMAT_BZIP2.asAllowableValue(),
                    CompressionInfo.COMPRESSION_FORMAT_XZ_LZMA2.asAllowableValue(),
                    CompressionInfo.COMPRESSION_FORMAT_LZMA.asAllowableValue(),
                    CompressionInfo.COMPRESSION_FORMAT_SNAPPY.asAllowableValue(),
                    CompressionInfo.COMPRESSION_FORMAT_SNAPPY_HADOOP.asAllowableValue(),
                    CompressionInfo.COMPRESSION_FORMAT_SNAPPY_FRAMED.asAllowableValue(),
                    CompressionInfo.COMPRESSION_FORMAT_LZ4_FRAMED.asAllowableValue(),
                    CompressionInfo.COMPRESSION_FORMAT_ZSTD.asAllowableValue(),
                    CompressionInfo.COMPRESSION_FORMAT_BROTLI.asAllowableValue())
            .defaultValue(CompressionInfo.COMPRESSION_FORMAT_NONE.getValue())
            .required(true)
            .build();

    public static final PropertyDescriptor COMPRESSION_LEVEL = new PropertyDescriptor.Builder()
            .name("Compression Level")
            .description("The compression level to use; this is valid only when using supported formats. A lower value results in faster processing "
                    + "but less compression; a value of 0 indicates no (that is, simple archiving) for gzip or minimal for xz-lzma2 compression."
                    + " Higher levels can mean much larger memory usage such as the case with levels 7-9 for xz-lzma/2 so be careful relative to heap size.")
            .defaultValue("1")
            .required(true)
            .allowableValues("0", "1", "2", "3", "4", "5", "6", "7", "8", "9")
            .dependsOn(OUTPUT_COMPRESSION,
                    CompressionInfo.COMPRESSION_FORMAT_ATTRIBUTE,
                    CompressionInfo.COMPRESSION_FORMAT_GZIP,
                    CompressionInfo.COMPRESSION_FORMAT_DEFLATE,
                    CompressionInfo.COMPRESSION_FORMAT_XZ_LZMA2,
                    CompressionInfo.COMPRESSION_FORMAT_ZSTD,
                    CompressionInfo.COMPRESSION_FORMAT_BROTLI)
            .build();

    public static final PropertyDescriptor UPDATE_FILENAME = new PropertyDescriptor.Builder()
            .name("Update Filename")
            .description("If true, will remove the filename extension when decompressing data (only if the extension indicates the appropriate "
                    + "compression format) and add the appropriate extension when compressing data")
            .required(true)
            .allowableValues("true", "false")
            .defaultValue("true")
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
        properties.add(INPUT_COMPRESSION);
        properties.add(OUTPUT_COMPRESSION);
        properties.add(COMPRESSION_LEVEL);
        properties.add(UPDATE_FILENAME);
        this.properties = Collections.unmodifiableList(properties);

        final Set<Relationship> relationships = new HashSet<>();
        relationships.add(REL_SUCCESS);
        relationships.add(REL_FAILURE);
        this.relationships = Collections.unmodifiableSet(relationships);

        final Map<String, String> mimeTypeMap = new HashMap<>();
        for(CompressionInfo compressionInfo : CompressionInfo.values()) {
            String[] mimeTypes = compressionInfo.getMimeTypes();
            if (mimeTypes == null) {
                continue;
            }
            for(String mimeType : mimeTypes) {
                mimeTypeMap.put(mimeType, compressionInfo.getValue());
            }
        }

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

        String decompressionFormatValue = context.getProperty(INPUT_COMPRESSION).getValue();
        if (decompressionFormatValue.equals(CompressionInfo.COMPRESSION_FORMAT_ATTRIBUTE.getValue())) {
            final String mimeType = flowFile.getAttribute(CoreAttributes.MIME_TYPE.key());
            if (mimeType == null) {
                logger.error("No {} attribute exists for {}; routing to failure", CoreAttributes.MIME_TYPE.key(), flowFile);
                session.transfer(flowFile, REL_FAILURE);
                return;
            }

            decompressionFormatValue = compressionFormatMimeTypeMap.get(mimeType);
            if (decompressionFormatValue == null) {
                logger.info("MIME Type of {} is '{}', which does not indicate a supported Decompression Format; routing to failure", flowFile, mimeType);
                session.transfer(flowFile, REL_FAILURE);
                return;
            }
        }

        String compressionFormatValue = context.getProperty(OUTPUT_COMPRESSION).getValue();
        if (compressionFormatValue.equals(CompressionInfo.COMPRESSION_FORMAT_ATTRIBUTE.getValue())) {
            final String mimeType = flowFile.getAttribute(CoreAttributes.MIME_TYPE.key());
            if (mimeType == null) {
                logger.error("No {} attribute exists for {}; routing to failure", CoreAttributes.MIME_TYPE.key(), flowFile);
                session.transfer(flowFile, REL_FAILURE);
                return;
            }

            compressionFormatValue = compressionFormatMimeTypeMap.get(mimeType);
            if (compressionFormatValue == null) {
                logger.info("MIME Type of {} is '{}', which does not indicate a supported Compression Format; routing to success without decompressing", flowFile, mimeType);
                session.transfer(flowFile, REL_SUCCESS);
                return;
            }
        }

        final String decompressionFormat = decompressionFormatValue;
        final String compressionFormat = compressionFormatValue;
        final AtomicReference<String> mimeTypeRef = new AtomicReference<>(null);
        final StopWatch stopWatch = new StopWatch(true);

        String inputFileExtension;
        String outputFileExtension;
        try {
            inputFileExtension = CompressionInfo.fromAllowableValue(decompressionFormat).getFileExtension();
        } catch (IllegalArgumentException iae) {
            inputFileExtension = "";
        }
        try {
            outputFileExtension = CompressionInfo.fromAllowableValue(compressionFormat).getFileExtension();
        } catch (IllegalArgumentException iae) {
            outputFileExtension = "";
        }

        try {
            flowFile = session.write(flowFile, (rawIn, rawOut) -> {
                final OutputStream compressionOut;
                final InputStream compressionIn;

                final OutputStream bufferedOut = new BufferedOutputStream(rawOut, STREAM_BUFFER_SIZE);
                final InputStream bufferedIn = new BufferedInputStream(rawIn, STREAM_BUFFER_SIZE);

                try {
                    // Decompress data by creating an InputStream for the decompression format, then recompress using the compression format
                    compressionIn = getCompressionInputStream(decompressionFormat, bufferedIn);

                    int compressionLevel = context.getProperty(COMPRESSION_LEVEL).asInteger();
                    compressionOut = getCompressionOutputStream(compressionFormat, compressionLevel, mimeTypeRef, bufferedOut);

                } catch (final Exception e) {
                    closeQuietly(bufferedOut);
                    throw new IOException(e);
                }

                StreamUtils.copy(compressionIn, compressionOut);
            });
            stopWatch.stop();

            final long sizeAfterCompression = flowFile.getSize();

            if (StringUtils.isEmpty(mimeTypeRef.get())) {
                flowFile = session.removeAttribute(flowFile, CoreAttributes.MIME_TYPE.key());
            } else {
                flowFile = session.putAttribute(flowFile, CoreAttributes.MIME_TYPE.key(), mimeTypeRef.get());
            }

            if (context.getProperty(UPDATE_FILENAME).asBoolean()) {
                String filename = flowFile.getAttribute(CoreAttributes.FILENAME.key());
                // Remove the input file extension if necessary
                if (filename.toLowerCase().endsWith(inputFileExtension)) {
                    filename = filename.substring(0, filename.length() - inputFileExtension.length());
                }
                flowFile = session.putAttribute(flowFile, CoreAttributes.FILENAME.key(), filename + outputFileExtension);
            }

            logger.info("Successfully recompressed {} using {} compression format; size changed from {} to {} bytes",
                    flowFile, compressionFormat, sizeBeforeCompression, sizeAfterCompression);
            session.getProvenanceReporter().modifyContent(flowFile, stopWatch.getDuration(TimeUnit.MILLISECONDS));
            session.transfer(flowFile, REL_SUCCESS);
        } catch (final ProcessException e) {
            logger.error("Unable to recompress {} using {} compression format due to {}; routing to failure", flowFile, compressionFormat, e);
            session.transfer(flowFile, REL_FAILURE);
        }
    }

    private void closeQuietly(final Closeable closeable) {
        if (closeable != null) {
            try {
                closeable.close();
            } catch (final Exception e) {
                // Ignore
            }
        }
    }

    private InputStream getCompressionInputStream(String decompressionFormat, InputStream parentInputStream)
            throws IOException, CompressorException {
        if (decompressionFormat.equalsIgnoreCase(CompressionInfo.COMPRESSION_FORMAT_LZMA.getValue())) {
            return new LzmaInputStream(parentInputStream, new Decoder());
        } else if (decompressionFormat.equalsIgnoreCase(CompressionInfo.COMPRESSION_FORMAT_XZ_LZMA2.getValue())) {
            return new XZInputStream(parentInputStream);
        } else if (decompressionFormat.equalsIgnoreCase(CompressionInfo.COMPRESSION_FORMAT_BZIP2.getValue())) {
            // need this two-arg constructor to support concatenated streams
            return new BZip2CompressorInputStream(parentInputStream, true);
        } else if (decompressionFormat.equalsIgnoreCase(CompressionInfo.COMPRESSION_FORMAT_GZIP.getValue())) {
            return new GzipCompressorInputStream(parentInputStream, true);
        } else if (decompressionFormat.equalsIgnoreCase(CompressionInfo.COMPRESSION_FORMAT_DEFLATE.getValue())) {
            return new InflaterInputStream(parentInputStream);
        } else if (decompressionFormat.equalsIgnoreCase(CompressionInfo.COMPRESSION_FORMAT_SNAPPY.getValue())) {
            return new SnappyInputStream(parentInputStream);
        } else if (decompressionFormat.equalsIgnoreCase(CompressionInfo.COMPRESSION_FORMAT_SNAPPY_HADOOP.getValue())) {
            throw new IOException("Cannot decompress snappy-hadoop.");
        } else if (decompressionFormat.equalsIgnoreCase(CompressionInfo.COMPRESSION_FORMAT_SNAPPY_FRAMED.getValue())) {
            return new SnappyFramedInputStream(parentInputStream);
        } else if (decompressionFormat.equalsIgnoreCase(CompressionInfo.COMPRESSION_FORMAT_LZ4_FRAMED.getValue())) {
            return new FramedLZ4CompressorInputStream(parentInputStream, true);
        } else if (decompressionFormat.equalsIgnoreCase(CompressionInfo.COMPRESSION_FORMAT_ZSTD.getValue())) {
            return new ZstdCompressorInputStream(parentInputStream);
        } else if (decompressionFormat.equalsIgnoreCase(CompressionInfo.COMPRESSION_FORMAT_BROTLI.getValue())) {
            Brotli4jLoader.ensureAvailability();
            return new BrotliInputStream(parentInputStream);
        } else if (decompressionFormat.equalsIgnoreCase(CompressionInfo.DECOMPRESSION_FORMAT_NONE.getValue())) {
            return parentInputStream;
        } else {
            return new CompressorStreamFactory().createCompressorInputStream(decompressionFormat.toLowerCase(), parentInputStream);
        }
    }

    private OutputStream getCompressionOutputStream(String compressionFormat, int compressionLevel, AtomicReference<String> mimeTypeRef, OutputStream parentOutputStream)
            throws IOException, CompressorException {
        final OutputStream compressionOut;
        if (compressionFormat.equalsIgnoreCase(CompressionInfo.COMPRESSION_FORMAT_GZIP.getValue())) {
            compressionOut = new GZIPOutputStream(parentOutputStream, compressionLevel);
            mimeTypeRef.set(CompressionInfo.COMPRESSION_FORMAT_GZIP.getMimeTypes()[0]);
        } else if (compressionFormat.equalsIgnoreCase(CompressionInfo.COMPRESSION_FORMAT_DEFLATE.getValue())) {
            compressionOut = new DeflaterOutputStream(parentOutputStream, new Deflater(compressionLevel));
            mimeTypeRef.set(CompressionInfo.COMPRESSION_FORMAT_GZIP.getMimeTypes()[0]);
        } else if (compressionFormat.equalsIgnoreCase(CompressionInfo.COMPRESSION_FORMAT_LZMA.getValue())) {
            compressionOut = new LzmaOutputStream.Builder(parentOutputStream).build();
            mimeTypeRef.set(CompressionInfo.COMPRESSION_FORMAT_LZMA.getMimeTypes()[0]);
        } else if (compressionFormat.equalsIgnoreCase(CompressionInfo.COMPRESSION_FORMAT_XZ_LZMA2.getValue())) {
            compressionOut = new XZOutputStream(parentOutputStream, new LZMA2Options(compressionLevel));
            mimeTypeRef.set(CompressionInfo.COMPRESSION_FORMAT_XZ_LZMA2.getMimeTypes()[0]);
        } else if (compressionFormat.equalsIgnoreCase(CompressionInfo.COMPRESSION_FORMAT_SNAPPY.getValue())) {
            compressionOut = new SnappyOutputStream(parentOutputStream);
            mimeTypeRef.set(CompressionInfo.COMPRESSION_FORMAT_SNAPPY.getMimeTypes()[0]);
        } else if (compressionFormat.equalsIgnoreCase(CompressionInfo.COMPRESSION_FORMAT_SNAPPY_HADOOP.getValue())) {
            compressionOut = new SnappyHadoopCompatibleOutputStream(parentOutputStream);
            mimeTypeRef.set(CompressionInfo.COMPRESSION_FORMAT_SNAPPY_HADOOP.getMimeTypes()[0]);
        } else if (compressionFormat.equalsIgnoreCase(CompressionInfo.COMPRESSION_FORMAT_SNAPPY_FRAMED.getValue())) {
            compressionOut = new SnappyFramedOutputStream(parentOutputStream);
            mimeTypeRef.set(CompressionInfo.COMPRESSION_FORMAT_SNAPPY_FRAMED.getMimeTypes()[0]);
        } else if (compressionFormat.equalsIgnoreCase(CompressionInfo.COMPRESSION_FORMAT_LZ4_FRAMED.getValue())) {
            compressionOut = new CompressorStreamFactory().createCompressorOutputStream(compressionFormat.toLowerCase(), parentOutputStream);
            mimeTypeRef.set(CompressionInfo.COMPRESSION_FORMAT_LZ4_FRAMED.getMimeTypes()[0]);
        } else if (compressionFormat.equalsIgnoreCase(CompressionInfo.COMPRESSION_FORMAT_ZSTD.getValue())) {
            final int zstdcompressionLevel = compressionLevel * 2;
            compressionOut = new ZstdCompressorOutputStream(parentOutputStream, zstdcompressionLevel);
            mimeTypeRef.set(CompressionInfo.COMPRESSION_FORMAT_ZSTD.getMimeTypes()[0]);
        } else if (compressionFormat.equalsIgnoreCase(CompressionInfo.COMPRESSION_FORMAT_BROTLI.getValue())) {
            Brotli4jLoader.ensureAvailability();
            Encoder.Parameters params = new Encoder.Parameters().setQuality(compressionLevel);
            compressionOut = new BrotliOutputStream(parentOutputStream, params);
            mimeTypeRef.set(CompressionInfo.COMPRESSION_FORMAT_BROTLI.getMimeTypes()[0]);
        } else if (compressionFormat.equalsIgnoreCase(CompressionInfo.COMPRESSION_FORMAT_BZIP2.getValue())) {
            compressionOut = new CompressorStreamFactory().createCompressorOutputStream(compressionFormat.toLowerCase(), parentOutputStream);
            mimeTypeRef.set(CompressionInfo.COMPRESSION_FORMAT_BZIP2.getMimeTypes()[0]);
        } else {
            compressionOut = parentOutputStream;
            mimeTypeRef.set("");
        }
        return compressionOut;
    }
}
