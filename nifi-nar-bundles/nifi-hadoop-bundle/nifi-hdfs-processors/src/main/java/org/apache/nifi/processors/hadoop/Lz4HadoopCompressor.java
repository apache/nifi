package org.apache.nifi.processors.hadoop;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.io.compress.BlockCompressorStream;
import org.apache.hadoop.io.compress.CompressionOutputStream;
import org.apache.hadoop.io.compress.lz4.Lz4Compressor;
import org.apache.nifi.annotation.behavior.SideEffectFree;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.ProcessorInitializationContext;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.io.StreamCallback;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.util.StopWatch;

/**
 * This class allows NiFi to generate Lz4-compressed data that may be loaded to HDFS.
 *
 * Per https://issues.apache.org/jira/browse/NIFI-3420 data compressed
 * using the Lz4 CLI is not readable by the native Hadoop Lz4 codec. This
 * processor adds the ability to convert data into a Hadoop-readable Lz4
 * data by using the actual Hadoop Lz4 codec to do the compression.
 */
@SideEffectFree
@Tags({"Lz4", "Hadoop", "HDFS"})
@CapabilityDescription("Compress data as Hadoop-readable lz4.")
public class Lz4HadoopCompressor extends AbstractProcessor
{
  public static final Relationship REL_SUCCESS = new Relationship.Builder().name("success").description("").build();

  private Set<Relationship> relationships;
  private List<PropertyDescriptor> properties;

  public static final PropertyDescriptor BUFFER_SIZE = new PropertyDescriptor.Builder()
      .name("Buffer Size")
      .defaultValue("" + 64 * 1024)
      .description("The size of the buffer to use for lz4 decompression, default 64k")
      .expressionLanguageSupported(true)
      .addValidator(StandardValidators.INTEGER_VALIDATOR)
      .required(true)
      .build();

  @Override
  public Set<Relationship> getRelationships() {
    return relationships;
  }

  @Override
  protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
    return properties;
  }

  @Override
  public void init(final ProcessorInitializationContext context)
  {
    final Set<Relationship> relationships = new HashSet<>();
    relationships.add(REL_SUCCESS);
    this.relationships = Collections.unmodifiableSet(relationships);

    final List<PropertyDescriptor> properties = new ArrayList<>();
    properties.add(BUFFER_SIZE);
    this.properties = Collections.unmodifiableList(properties);
  }

  @Override
  public void onTrigger(ProcessContext context, ProcessSession session) throws ProcessException
  {
    FlowFile flowFile = session.get();

    if (flowFile == null) {
      return;
    }

    final int bufferSize = context.getProperty(BUFFER_SIZE).evaluateAttributeExpressions(flowFile).asInteger();

    // Use a buffer to store compressed data
    final DataOutputBuffer compressedDataBuffer = new DataOutputBuffer(bufferSize);

    // Raw buffer to use for reading from the input stream -> passed to block compression algorithm
    final byte[] rawBuffer = new byte[bufferSize];

    int compressionOverhead = (bufferSize / 6) + 32;

    // Define the stream that does the compression and outputs to a compressedDataBuffer
    // to store data
    final CompressionOutputStream compressedStream = new BlockCompressorStream(
        compressedDataBuffer, new Lz4Compressor(bufferSize), bufferSize, compressionOverhead);

    // Wrap the compression stream in a data output stream
    final DataOutputStream compressedOut = new DataOutputStream(new BufferedOutputStream(compressedStream));

    try {
      final StopWatch stopWatch = new StopWatch(true);

      session.write(flowFile, new StreamCallback()
      {
        @Override
        public void process(InputStream in, OutputStream out) throws IOException
        {
          try (final BufferedInputStream reader = new BufferedInputStream(in, bufferSize);
              final BufferedOutputStream writer = new BufferedOutputStream(out, bufferSize)) {
            compressedDataBuffer.reset();
            int bytesRead;

            while ((bytesRead = reader.read(rawBuffer, 0, bufferSize)) != -1) {
              // Write the output of the compression
              compressedOut.write(rawBuffer, 0, bytesRead);
              compressedOut.flush();

              compressedStream.finish();

              writer.write(compressedDataBuffer.getData(), 0, compressedDataBuffer.getLength());
            }

            writer.flush();
          } catch (Exception ex) {
            ex.printStackTrace();
            getLogger().error("Failed to output compressed data");
          }
        }
      });

      session.getProvenanceReporter().modifyContent(flowFile, stopWatch.getElapsed(TimeUnit.MILLISECONDS));
      getLogger().info("Succesfully compressed bytes to lz4");
    } catch (final Exception e) {
      throw new ProcessException(e);
    } finally {
      try {
        compressedOut.close();
      } catch (IOException e) {
        getLogger().error("Failed to close compressed output stream");
      }
    }

    session.transfer(flowFile, REL_SUCCESS);
  }
}
