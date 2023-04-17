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

import com.aayushatharva.brotli4j.Brotli4jLoader;
import com.aayushatharva.brotli4j.decoder.BrotliInputStream;
import com.aayushatharva.brotli4j.encoder.BrotliOutputStream;
import com.aayushatharva.brotli4j.encoder.Encoder;
import lzma.sdk.lzma.Decoder;
import lzma.streams.LzmaInputStream;
import lzma.streams.LzmaOutputStream;
import org.apache.commons.compress.compressors.CompressorStreamFactory;
import org.apache.commons.compress.compressors.bzip2.BZip2CompressorInputStream;
import org.apache.commons.compress.compressors.gzip.GzipCompressorInputStream;
import org.apache.commons.compress.compressors.lz4.FramedLZ4CompressorInputStream;
import org.apache.commons.compress.compressors.zstandard.ZstdCompressorInputStream;
import org.apache.commons.compress.compressors.zstandard.ZstdCompressorOutputStream;
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
import org.apache.nifi.stream.io.GZIPOutputStream;
import org.apache.nifi.util.StopWatch;
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

@EventDriven
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

    public static final String DECOMPRESSION_FORMAT_NONE = "no decompression";
    public static final String COMPRESSION_FORMAT_NONE = "no compression";
    public static final String COMPRESSION_FORMAT_ATTRIBUTE = "use mime.type attribute";
    public static final String COMPRESSION_FORMAT_GZIP = "gzip";
    public static final String COMPRESSION_FORMAT_DEFLATE = "deflate";
    public static final String COMPRESSION_FORMAT_BZIP2 = "bzip2";
    public static final String COMPRESSION_FORMAT_XZ_LZMA2 = "xz-lzma2";
    public static final String COMPRESSION_FORMAT_LZMA = "lzma";
    public static final String COMPRESSION_FORMAT_SNAPPY = "snappy";
    public static final String COMPRESSION_FORMAT_SNAPPY_HADOOP = "snappy-hadoop";
    public static final String COMPRESSION_FORMAT_SNAPPY_FRAMED = "snappy framed";
    public static final String COMPRESSION_FORMAT_LZ4_FRAMED = "lz4-framed";
    public static final String COMPRESSION_FORMAT_ZSTD = "zstd";
    public static final String COMPRESSION_FORMAT_BROTLI = "brotli";

    public static final PropertyDescriptor DECOMPRESSION_FORMAT = new PropertyDescriptor.Builder()
            .name("Decompression Format")
            .description("The decompression format to use for input FlowFiles. Valid values are: None, GZIP, Deflate, ZSTD, BZIP2, XZ-LZMA2, LZMA, Brotli, Snappy, Snappy Framed, and LZ4-Framed")
            .allowableValues(DECOMPRESSION_FORMAT_NONE, COMPRESSION_FORMAT_ATTRIBUTE, COMPRESSION_FORMAT_GZIP, COMPRESSION_FORMAT_DEFLATE, COMPRESSION_FORMAT_BZIP2,
                    COMPRESSION_FORMAT_XZ_LZMA2, COMPRESSION_FORMAT_LZMA, COMPRESSION_FORMAT_SNAPPY, COMPRESSION_FORMAT_SNAPPY_FRAMED,
                    COMPRESSION_FORMAT_LZ4_FRAMED, COMPRESSION_FORMAT_ZSTD, COMPRESSION_FORMAT_BROTLI)
            .defaultValue(DECOMPRESSION_FORMAT_NONE)
            .required(true)
            .build();
    public static final PropertyDescriptor COMPRESSION_FORMAT = new PropertyDescriptor.Builder()
            .name("Compression Format")
            .description("The compression format to use for output FlowFiles. Valid values are: None, GZIP, Deflate, ZSTD, BZIP2, XZ-LZMA2, LZMA, Brotli, Snappy, Snappy Hadoop, "
                    + "Snappy Framed, and LZ4-Framed")
            .allowableValues(COMPRESSION_FORMAT_NONE, COMPRESSION_FORMAT_GZIP, COMPRESSION_FORMAT_DEFLATE, COMPRESSION_FORMAT_BZIP2,
                    COMPRESSION_FORMAT_XZ_LZMA2, COMPRESSION_FORMAT_LZMA, COMPRESSION_FORMAT_SNAPPY, COMPRESSION_FORMAT_SNAPPY_HADOOP, COMPRESSION_FORMAT_SNAPPY_FRAMED,
                    COMPRESSION_FORMAT_LZ4_FRAMED, COMPRESSION_FORMAT_ZSTD, COMPRESSION_FORMAT_BROTLI)
            .defaultValue(COMPRESSION_FORMAT_NONE)
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
            .dependsOn(COMPRESSION_FORMAT, COMPRESSION_FORMAT_ATTRIBUTE, COMPRESSION_FORMAT_GZIP, COMPRESSION_FORMAT_DEFLATE,
                    COMPRESSION_FORMAT_XZ_LZMA2, COMPRESSION_FORMAT_ZSTD, COMPRESSION_FORMAT_BROTLI)
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
        properties.add(DECOMPRESSION_FORMAT);
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
        mimeTypeMap.put("application/deflate", COMPRESSION_FORMAT_DEFLATE);
        mimeTypeMap.put("application/x-deflate", COMPRESSION_FORMAT_DEFLATE);
        mimeTypeMap.put("application/bzip2", COMPRESSION_FORMAT_BZIP2);
        mimeTypeMap.put("application/x-bzip2", COMPRESSION_FORMAT_BZIP2);
        mimeTypeMap.put("application/x-lzma", COMPRESSION_FORMAT_LZMA);
        mimeTypeMap.put("application/x-snappy", COMPRESSION_FORMAT_SNAPPY);
        mimeTypeMap.put("application/x-snappy-hadoop", COMPRESSION_FORMAT_SNAPPY_HADOOP);
        mimeTypeMap.put("application/x-snappy-framed", COMPRESSION_FORMAT_SNAPPY_FRAMED);
        mimeTypeMap.put("application/x-lz4-framed", COMPRESSION_FORMAT_LZ4_FRAMED);
        mimeTypeMap.put("application/zstd", COMPRESSION_FORMAT_ZSTD);
        mimeTypeMap.put("application/x-brotli", COMPRESSION_FORMAT_BROTLI);
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

        String decompressionFormatValue = context.getProperty(DECOMPRESSION_FORMAT).getValue();
        if (decompressionFormatValue.equals(COMPRESSION_FORMAT_ATTRIBUTE)) {
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

        String compressionFormatValue = context.getProperty(COMPRESSION_FORMAT).getValue();
        if (compressionFormatValue.equals(COMPRESSION_FORMAT_ATTRIBUTE)) {
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

        final String inputFileExtension = getFileExtension(decompressionFormat);
        final String outputFileExtension = getFileExtension(compressionFormat);

        try {
            flowFile = session.write(flowFile, (rawIn, rawOut) -> {
                final OutputStream compressionOut;
                final InputStream compressionIn;

                final OutputStream bufferedOut = new BufferedOutputStream(rawOut, 65536);
                final InputStream bufferedIn = new BufferedInputStream(rawIn, 65536);

                try {
                    // Decompress data by creating an InputStream for the decompression format, then recompress using the compression format
                    switch (decompressionFormat.toLowerCase()) {
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
                        case COMPRESSION_FORMAT_DEFLATE:
                            compressionIn = new InflaterInputStream(bufferedIn);
                            break;
                        case COMPRESSION_FORMAT_SNAPPY:
                            compressionIn = new SnappyInputStream(bufferedIn);
                            break;
                        case COMPRESSION_FORMAT_SNAPPY_HADOOP:
                            throw new Exception("Cannot decompress snappy-hadoop.");
                        case COMPRESSION_FORMAT_SNAPPY_FRAMED:
                            compressionIn = new SnappyFramedInputStream(bufferedIn);
                            break;
                        case COMPRESSION_FORMAT_LZ4_FRAMED:
                            compressionIn = new FramedLZ4CompressorInputStream(bufferedIn, true);
                            break;
                        case COMPRESSION_FORMAT_ZSTD:
                            compressionIn = new ZstdCompressorInputStream(bufferedIn);
                            break;
                        case COMPRESSION_FORMAT_BROTLI:
                            Brotli4jLoader.ensureAvailability();
                            compressionIn = new BrotliInputStream(bufferedIn);
                            break;
                        case DECOMPRESSION_FORMAT_NONE:
                            compressionIn = bufferedIn;
                            break;
                        default:
                            compressionIn = new CompressorStreamFactory().createCompressorInputStream(decompressionFormat.toLowerCase(), bufferedIn);
                    }

                    switch (compressionFormat.toLowerCase()) {
                        case COMPRESSION_FORMAT_GZIP:
                            int compressionLevel = context.getProperty(COMPRESSION_LEVEL).asInteger();
                            compressionOut = new GZIPOutputStream(bufferedOut, compressionLevel);
                            mimeTypeRef.set("application/gzip");
                            break;
                        case COMPRESSION_FORMAT_DEFLATE:
                            compressionLevel = context.getProperty(COMPRESSION_LEVEL).asInteger();
                            compressionOut = new DeflaterOutputStream(bufferedOut, new Deflater(compressionLevel));
                            mimeTypeRef.set("application/gzip");
                            break;
                        case COMPRESSION_FORMAT_LZMA:
                            compressionOut = new LzmaOutputStream.Builder(bufferedOut).build();
                            mimeTypeRef.set("application/x-lzma");
                            break;
                        case COMPRESSION_FORMAT_XZ_LZMA2:
                            final int xzCompressionLevel = context.getProperty(COMPRESSION_LEVEL).asInteger();
                            compressionOut = new XZOutputStream(bufferedOut, new LZMA2Options(xzCompressionLevel));
                            mimeTypeRef.set("application/x-xz");
                            break;
                        case COMPRESSION_FORMAT_SNAPPY:
                            compressionOut = new SnappyOutputStream(bufferedOut);
                            mimeTypeRef.set("application/x-snappy");
                            break;
                        case COMPRESSION_FORMAT_SNAPPY_HADOOP:
                            compressionOut = new SnappyHadoopCompatibleOutputStream(bufferedOut);
                            mimeTypeRef.set("application/x-snappy-hadoop");
                            break;
                        case COMPRESSION_FORMAT_SNAPPY_FRAMED:
                            compressionOut = new SnappyFramedOutputStream(bufferedOut);
                            mimeTypeRef.set("application/x-snappy-framed");
                            break;
                        case COMPRESSION_FORMAT_LZ4_FRAMED:
                            mimeTypeRef.set("application/x-lz4-framed");
                            compressionOut = new CompressorStreamFactory().createCompressorOutputStream(compressionFormat.toLowerCase(), bufferedOut);
                            break;
                        case COMPRESSION_FORMAT_ZSTD:
                            final int zstdcompressionLevel = context.getProperty(COMPRESSION_LEVEL).asInteger() * 2;
                            compressionOut = new ZstdCompressorOutputStream(bufferedOut, zstdcompressionLevel);
                            mimeTypeRef.set("application/zstd");
                            break;
                        case COMPRESSION_FORMAT_BROTLI:
                            Brotli4jLoader.ensureAvailability();
                            compressionLevel = context.getProperty(COMPRESSION_LEVEL).asInteger();
                            Encoder.Parameters params = new Encoder.Parameters().setQuality(compressionLevel);
                            compressionOut = new BrotliOutputStream(bufferedOut, params);
                            mimeTypeRef.set("application/x-brotli");
                            break;
                        case COMPRESSION_FORMAT_BZIP2:
                            mimeTypeRef.set("application/x-bzip2");
                            compressionOut = new CompressorStreamFactory().createCompressorOutputStream(compressionFormat.toLowerCase(), bufferedOut);
                            break;
                        case COMPRESSION_FORMAT_NONE:
                        default:
                            compressionOut = bufferedOut;
                            break;
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
            });
            stopWatch.stop();

            final long sizeAfterCompression = flowFile.getSize();

            if (mimeTypeRef.get() == null) {
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

    private String getFileExtension(String format) {
        switch (format.toLowerCase()) {
            case COMPRESSION_FORMAT_GZIP:
                return ".gz";
            case COMPRESSION_FORMAT_DEFLATE:
                return ".zlib";
            case COMPRESSION_FORMAT_LZMA:
                return ".lzma";
            case COMPRESSION_FORMAT_XZ_LZMA2:
                return ".xz";
            case COMPRESSION_FORMAT_BZIP2:
                return ".bz2";
            case COMPRESSION_FORMAT_SNAPPY:
                return ".snappy";
            case COMPRESSION_FORMAT_SNAPPY_HADOOP:
                return ".snappy";
            case COMPRESSION_FORMAT_SNAPPY_FRAMED:
                return ".sz";
            case COMPRESSION_FORMAT_LZ4_FRAMED:
                return ".lz4";
            case COMPRESSION_FORMAT_ZSTD:
                return ".zst";
            case COMPRESSION_FORMAT_BROTLI:
                return ".br";
            default:
                return "";
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
}
