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
package org.apache.hadoop.hive.ql.io.orc;

import static com.google.common.base.Preconditions.checkArgument;

import java.io.IOException;
import java.io.OutputStream;
import java.lang.management.ManagementFactory;
import java.nio.ByteBuffer;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.TimeZone;
import java.util.TreeMap;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.common.JavaUtils;
import org.apache.hadoop.hive.common.type.HiveDecimal;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.exec.vector.ColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.hadoop.hive.ql.io.IOConstants;
import org.apache.hadoop.hive.ql.io.filters.BloomFilterIO;
import org.apache.hadoop.hive.ql.io.orc.CompressionCodec.Modifier;
import org.apache.hadoop.hive.ql.io.orc.OrcFile.CompressionStrategy;
import org.apache.hadoop.hive.ql.io.orc.OrcFile.EncodingStrategy;
import org.apache.hadoop.hive.ql.io.orc.OrcProto.RowIndexEntry;
import org.apache.hadoop.hive.ql.io.orc.OrcProto.StripeStatistics;
import org.apache.hadoop.hive.ql.io.orc.OrcProto.Type;
import org.apache.hadoop.hive.ql.io.orc.OrcProto.UserMetadataItem;
import org.apache.hadoop.hive.ql.util.JavaDataModel;
import org.apache.hadoop.hive.serde2.io.DateWritable;
import org.apache.hadoop.hive.serde2.objectinspector.ListObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.MapObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.UnionObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.BinaryObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.BooleanObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.ByteObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.DateObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.DoubleObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.FloatObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.HiveCharObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.HiveDecimalObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.HiveVarcharObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.IntObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.LongObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.ShortObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.StringObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.TimestampObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.CharTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.DecimalTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.VarcharTypeInfo;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Joiner;
import com.google.common.collect.Lists;
import com.google.common.primitives.Longs;
import com.google.protobuf.ByteString;
import com.google.protobuf.CodedOutputStream;
import org.apache.nifi.stream.io.ByteCountingOutputStream;

/**
 * An ORC file writer. The file is divided into stripes, which is the natural
 * unit of work when reading. Each stripe is buffered in memory until the
 * memory reaches the stripe size and then it is written out broken down by
 * columns. Each column is written by a TreeWriter that is specific to that
 * type of column. TreeWriters may have children TreeWriters that handle the
 * sub-types. Each of the TreeWriters writes the column's data as a set of
 * streams.
 * <p>
 * This class is synchronized so that multi-threaded access is ok. In
 * particular, because the MemoryManager is shared between writers, this class
 * assumes that checkMemory may be called from a separate thread.
 */
public class OrcFlowFileWriter implements Writer, MemoryManager.Callback {

    private static final Log LOG = LogFactory.getLog(WriterImpl.class);

    private static final int HDFS_BUFFER_SIZE = 256 * 1024;
    private static final int MIN_ROW_INDEX_STRIDE = 1000;

    // threshold above which buffer size will be automatically resized
    private static final int COLUMN_COUNT_THRESHOLD = 1000;

    private final Path path;
    private final long defaultStripeSize;
    private long adjustedStripeSize;
    private final int rowIndexStride;
    private final CompressionKind compress;
    private final CompressionCodec codec;
    private final boolean addBlockPadding;
    private final int bufferSize;
    private final long blockSize;
    private final float paddingTolerance;
    // the streams that make up the current stripe
    private final Map<StreamName, BufferedStream> streams = new TreeMap<>();

    private final OutputStream flowFileOutputStream;

    private ByteCountingOutputStream rawWriter = null;
    // the compressed metadata information outStream
    private OutStream writer = null;
    // a protobuf outStream around streamFactory
    private CodedOutputStream protobufWriter = null;
    private long headerLength;
    private int columnCount;
    private long rowCount = 0;
    private long rowsInStripe = 0;
    private long rawDataSize = 0;
    private int rowsInIndex = 0;
    private int stripesAtLastFlush = -1;
    private final List<OrcProto.StripeInformation> stripes = new ArrayList<>();
    private final Map<String, ByteString> userMetadata = new TreeMap<>();
    private final StreamFactory streamFactory = new StreamFactory();
    private final TreeWriter treeWriter;
    private final boolean buildIndex;
    private final MemoryManager memoryManager;
    private final OrcFile.Version version;
    private final Configuration conf;
    private final OrcFile.WriterCallback callback;
    private final OrcFile.WriterContext callbackContext;
    private final OrcFile.EncodingStrategy encodingStrategy;
    private final OrcFile.CompressionStrategy compressionStrategy;
    private final boolean[] bloomFilterColumns;
    private final double bloomFilterFpp;


    public OrcFlowFileWriter(OutputStream flowFileOutputStream,
                             Path path,
                             Configuration conf,
                             ObjectInspector inspector,
                             long stripeSize,
                             CompressionKind compress,
                             int bufferSize,
                             int rowIndexStride,
                             MemoryManager memoryManager,
                             boolean addBlockPadding,
                             OrcFile.Version version,
                             OrcFile.WriterCallback callback,
                             EncodingStrategy encodingStrategy,
                             CompressionStrategy compressionStrategy,
                             float paddingTolerance,
                             long blockSizeValue,
                             String bloomFilterColumnNames,
                             double bloomFilterFpp) throws IOException {
        this.flowFileOutputStream = flowFileOutputStream;
        this.path = path;
        this.conf = conf;
        this.callback = callback;
        callbackContext = (callback != null) ? () -> OrcFlowFileWriter.this : null;
        this.adjustedStripeSize = stripeSize;
        this.defaultStripeSize = stripeSize;
        this.version = version;
        this.encodingStrategy = encodingStrategy;
        this.compressionStrategy = compressionStrategy;
        this.addBlockPadding = addBlockPadding;
        this.blockSize = blockSizeValue;
        this.paddingTolerance = paddingTolerance;
        this.compress = compress;
        this.rowIndexStride = rowIndexStride;
        this.memoryManager = memoryManager;
        buildIndex = rowIndexStride > 0;
        codec = createCodec(compress);
        String allColumns = conf.get(IOConstants.COLUMNS);
        if (allColumns == null) {
            allColumns = getColumnNamesFromInspector(inspector);
        }
        this.bufferSize = getEstimatedBufferSize(allColumns, bufferSize);
        if (version == OrcFile.Version.V_0_11) {
            /* do not write bloom filters for ORC v11 */
            this.bloomFilterColumns =
                    OrcUtils.includeColumns(null, allColumns, inspector);
        } else {
            this.bloomFilterColumns =
                    OrcUtils.includeColumns(bloomFilterColumnNames, allColumns, inspector);
        }
        this.bloomFilterFpp = bloomFilterFpp;
        treeWriter = createTreeWriter(inspector, streamFactory, false);
        if (buildIndex && rowIndexStride < MIN_ROW_INDEX_STRIDE) {
            throw new IllegalArgumentException("Row stride must be at least " +
                    MIN_ROW_INDEX_STRIDE);
        }

        // ensure that we are able to handle callbacks before we register ourselves
        memoryManager.addWriter(path, stripeSize, this);
    }

    private String getColumnNamesFromInspector(ObjectInspector inspector) {
        List<String> fieldNames = Lists.newArrayList();
        Joiner joiner = Joiner.on(",");
        if (inspector instanceof StructObjectInspector) {
            StructObjectInspector soi = (StructObjectInspector) inspector;
            List<? extends StructField> fields = soi.getAllStructFieldRefs();
            fieldNames.addAll(fields.stream().map((Function<StructField, String>) StructField::getFieldName).collect(Collectors.toList()));
        }
        return joiner.join(fieldNames);
    }

    @VisibleForTesting
    int getEstimatedBufferSize(int bs) {
        return getEstimatedBufferSize(conf.get(IOConstants.COLUMNS), bs);
    }

    int getEstimatedBufferSize(String colNames, int bs) {
        long availableMem = getMemoryAvailableForORC();
        if (colNames != null) {
            final int numCols = colNames.split(",").length;
            if (numCols > COLUMN_COUNT_THRESHOLD) {
                // In BufferedStream, there are 3 outstream buffers (compressed,
                // uncompressed and overflow) and list of previously compressed buffers.
                // Since overflow buffer is rarely used, lets consider only 2 allocation.
                // Also, initially, the list of compression buffers will be empty.
                final int outStreamBuffers = codec == null ? 1 : 2;

                // max possible streams per column is 5. For string columns, there is
                // ROW_INDEX, PRESENT, DATA, LENGTH, DICTIONARY_DATA streams.
                final int maxStreams = 5;

                // Lets assume 10% memory for holding dictionary in memory and other
                // object allocations
                final long miscAllocation = (long) (0.1f * availableMem);

                // compute the available memory
                final long remainingMem = availableMem - miscAllocation;

                int estBufferSize = (int) (remainingMem / (maxStreams * outStreamBuffers * numCols));
                estBufferSize = getClosestBufferSize(estBufferSize, bs);
                if (estBufferSize > bs) {
                    estBufferSize = bs;
                }

                LOG.info("WIDE TABLE - Number of columns: " + numCols + " Chosen compression buffer size: " + estBufferSize);
                return estBufferSize;
            }
        }
        return bs;
    }

    private int getClosestBufferSize(int estBufferSize, int bs) {
        final int kb4 = 4 * 1024;
        final int kb8 = 8 * 1024;
        final int kb16 = 16 * 1024;
        final int kb32 = 32 * 1024;
        final int kb64 = 64 * 1024;
        final int kb128 = 128 * 1024;
        final int kb256 = 256 * 1024;
        if (estBufferSize <= kb4) {
            return kb4;
        } else if (estBufferSize > kb4 && estBufferSize <= kb8) {
            return kb8;
        } else if (estBufferSize > kb8 && estBufferSize <= kb16) {
            return kb16;
        } else if (estBufferSize > kb16 && estBufferSize <= kb32) {
            return kb32;
        } else if (estBufferSize > kb32 && estBufferSize <= kb64) {
            return kb64;
        } else if (estBufferSize > kb64 && estBufferSize <= kb128) {
            return kb128;
        } else {
            return kb256;
        }
    }

    // the assumption is only one ORC writer open at a time, which holds true for
    // most of the cases. HIVE-6455 forces single writer case.
    private long getMemoryAvailableForORC() {
        HiveConf.ConfVars poolVar = HiveConf.ConfVars.HIVE_ORC_FILE_MEMORY_POOL;
        double maxLoad = conf.getFloat(poolVar.varname, poolVar.defaultFloatVal);
        long totalMemoryPool = Math.round(ManagementFactory.getMemoryMXBean().getHeapMemoryUsage().getMax() * maxLoad);
        return totalMemoryPool;
    }

    public static CompressionCodec createCodec(CompressionKind kind) {
        switch (kind) {
            case NONE:
                return null;
            case ZLIB:
                return new ZlibCodec();
            case SNAPPY:
                return new SnappyCodec();
            case LZO:
                try {
                    Class<? extends CompressionCodec> lzo =
                            (Class<? extends CompressionCodec>)
                                    JavaUtils.loadClass("org.apache.hadoop.hive.ql.io.orc.LzoCodec");
                    return lzo.newInstance();
                } catch (ClassNotFoundException e) {
                    throw new IllegalArgumentException("LZO is not available.", e);
                } catch (InstantiationException e) {
                    throw new IllegalArgumentException("Problem initializing LZO", e);
                } catch (IllegalAccessException e) {
                    throw new IllegalArgumentException("Insufficient access to LZO", e);
                }
            default:
                throw new IllegalArgumentException("Unknown compression codec: " +
                        kind);
        }
    }

    @Override
    public synchronized boolean checkMemory(double newScale) throws IOException {
        long limit = Math.round(adjustedStripeSize * newScale);
        long size = estimateStripeSize();
        if (LOG.isDebugEnabled()) {
            LOG.debug("ORC writer " + path + " size = " + size + " limit = " +
                    limit);
        }
        if (size > limit) {
            flushStripe();
            return true;
        }
        return false;
    }

    /**
     * This class is used to hold the contents of streams as they are buffered.
     * The TreeWriters write to the outStream and the codec compresses the
     * data as buffers fill up and stores them in the output list. When the
     * stripe is being written, the whole stream is written to the file.
     */
    private class BufferedStream implements OutStream.OutputReceiver {
        private final OutStream outStream;
        private final List<ByteBuffer> output = new ArrayList<ByteBuffer>();

        BufferedStream(String name, int bufferSize,
                       CompressionCodec codec) throws IOException {
            outStream = new OutStream(name, bufferSize, codec, this);
        }

        /**
         * Receive a buffer from the compression codec.
         *
         * @param buffer the buffer to save
         * @throws IOException if an error occurs while receiving a buffer
         */
        @Override
        public void output(ByteBuffer buffer) {
            output.add(buffer);
        }

        /**
         * Get the number of bytes in buffers that are allocated to this stream.
         *
         * @return number of bytes in buffers
         */
        public long getBufferSize() {
            long result = 0;
            for (ByteBuffer buf : output) {
                result += buf.capacity();
            }
            return outStream.getBufferSize() + result;
        }

        /**
         * Flush the stream to the codec.
         *
         * @throws IOException if an error occurs while flushing the stream
         */
        public void flush() throws IOException {
            outStream.flush();
        }

        /**
         * Clear all of the buffers.
         *
         * @throws IOException if an error occurs while clearing the buffers
         */
        public void clear() throws IOException {
            outStream.clear();
            output.clear();
        }

        /**
         * Check the state of suppress flag in output stream
         *
         * @return value of suppress flag
         */
        public boolean isSuppressed() {
            return outStream.isSuppressed();
        }

        /**
         * Get the number of bytes that will be written to the output. Assumes
         * the stream has already been flushed.
         *
         * @return the number of bytes
         */
        public long getOutputSize() {
            long result = 0;
            for (ByteBuffer buffer : output) {
                result += buffer.remaining();
            }
            return result;
        }

        /**
         * Write the saved compressed buffers to the OutputStream.
         *
         * @param out the stream to write to
         * @throws IOException if an error occurs during write
         */
        void spillTo(OutputStream out) throws IOException {
            for (ByteBuffer buffer : output) {
                out.write(buffer.array(), buffer.arrayOffset() + buffer.position(),
                        buffer.remaining());
            }
        }

        @Override
        public String toString() {
            return outStream.toString();
        }
    }

    /**
     * An output receiver that writes the ByteBuffers to the output stream
     * as they are received.
     */
    private class DirectStream implements OutStream.OutputReceiver {
        private final OutputStream output;

        DirectStream(OutputStream output) {
            this.output = output;
        }

        @Override
        public void output(ByteBuffer buffer) throws IOException {
            output.write(buffer.array(), buffer.arrayOffset() + buffer.position(),
                    buffer.remaining());
        }
    }

    private static class RowIndexPositionRecorder implements PositionRecorder {
        private final OrcProto.RowIndexEntry.Builder builder;

        RowIndexPositionRecorder(OrcProto.RowIndexEntry.Builder builder) {
            this.builder = builder;
        }

        @Override
        public void addPosition(long position) {
            builder.addPositions(position);
        }
    }

    /**
     * Interface from the Writer to the TreeWriters. This limits the visibility
     * that the TreeWriters have into the Writer.
     */
    private class StreamFactory {
        /**
         * Create a stream to store part of a column.
         *
         * @param column the column id for the stream
         * @param kind   the kind of stream
         * @return The output outStream that the section needs to be written to.
         * @throws IOException if an error occurs while creating the stream
         */
        public OutStream createStream(int column,
                                      OrcProto.Stream.Kind kind
        ) throws IOException {
            final StreamName name = new StreamName(column, kind);
            final EnumSet<CompressionCodec.Modifier> modifiers;

            switch (kind) {
                case BLOOM_FILTER:
                case DATA:
                case DICTIONARY_DATA:
                    if (getCompressionStrategy() == CompressionStrategy.SPEED) {
                        modifiers = EnumSet.of(Modifier.FAST, Modifier.TEXT);
                    } else {
                        modifiers = EnumSet.of(Modifier.DEFAULT, Modifier.TEXT);
                    }
                    break;
                case LENGTH:
                case DICTIONARY_COUNT:
                case PRESENT:
                case ROW_INDEX:
                case SECONDARY:
                    // easily compressed using the fastest modes
                    modifiers = EnumSet.of(Modifier.FASTEST, Modifier.BINARY);
                    break;
                default:
                    LOG.warn("Missing ORC compression modifiers for " + kind);
                    modifiers = null;
                    break;
            }

            BufferedStream result = streams.get(name);
            if (result == null) {
                result = new BufferedStream(name.toString(), bufferSize,
                        codec == null ? codec : codec.modify(modifiers));
                streams.put(name, result);
            }
            return result.outStream;
        }

        /**
         * Get the next column id.
         *
         * @return a number from 0 to the number of columns - 1
         */
        public int getNextColumnId() {
            return columnCount++;
        }

        /**
         * Get the current column id. After creating all tree writers this count should tell how many
         * columns (including columns within nested complex objects) are created in total.
         *
         * @return current column id
         */
        public int getCurrentColumnId() {
            return columnCount;
        }

        /**
         * Get the stride rate of the row index.
         */
        public int getRowIndexStride() {
            return rowIndexStride;
        }

        /**
         * Should be building the row index.
         *
         * @return true if we are building the index
         */
        public boolean buildIndex() {
            return buildIndex;
        }

        /**
         * Is the ORC file compressed?
         *
         * @return are the streams compressed
         */
        public boolean isCompressed() {
            return codec != null;
        }

        /**
         * Get the encoding strategy to use.
         *
         * @return encoding strategy
         */
        public EncodingStrategy getEncodingStrategy() {
            return encodingStrategy;
        }

        /**
         * Get the compression strategy to use.
         *
         * @return compression strategy
         */
        public CompressionStrategy getCompressionStrategy() {
            return compressionStrategy;
        }

        /**
         * Get the bloom filter columns
         *
         * @return bloom filter columns
         */
        public boolean[] getBloomFilterColumns() {
            return bloomFilterColumns;
        }

        /**
         * Get bloom filter false positive percentage.
         *
         * @return fpp
         */
        public double getBloomFilterFPP() {
            return bloomFilterFpp;
        }

        /**
         * Get the writer's configuration.
         *
         * @return configuration
         */
        public Configuration getConfiguration() {
            return conf;
        }

        /**
         * Get the version of the file to write.
         */
        public OrcFile.Version getVersion() {
            return version;
        }
    }

    /**
     * The parent class of all of the writers for each column. Each column
     * is written by an instance of this class. The compound types (struct,
     * list, map, and union) have children tree writers that write the children
     * types.
     */
    private abstract static class TreeWriter {
        protected final int id;
        protected final ObjectInspector inspector;
        private final BitFieldWriter isPresent;
        private final boolean isCompressed;
        protected final ColumnStatisticsImpl indexStatistics;
        protected final ColumnStatisticsImpl stripeColStatistics;
        private final ColumnStatisticsImpl fileStatistics;
        protected TreeWriter[] childrenWriters;
        protected final RowIndexPositionRecorder rowIndexPosition;
        private final OrcProto.RowIndex.Builder rowIndex;
        private final OrcProto.RowIndexEntry.Builder rowIndexEntry;
        private final PositionedOutputStream rowIndexStream;
        private final PositionedOutputStream bloomFilterStream;
        protected final BloomFilterIO bloomFilter;
        protected final boolean createBloomFilter;
        private final OrcProto.BloomFilterIndex.Builder bloomFilterIndex;
        private final OrcProto.BloomFilter.Builder bloomFilterEntry;
        private boolean foundNulls;
        private OutStream isPresentOutStream;
        private final List<StripeStatistics.Builder> stripeStatsBuilders;

        /**
         * Create a tree writer.
         *
         * @param columnId      the column id of the column to write
         * @param inspector     the object inspector to use
         * @param streamFactory limited access to the Writer's data.
         * @param nullable      can the value be null?
         * @throws IOException if an error occurs during creation
         */
        TreeWriter(int columnId, ObjectInspector inspector,
                   StreamFactory streamFactory,
                   boolean nullable) throws IOException {
            this.isCompressed = streamFactory.isCompressed();
            this.id = columnId;
            this.inspector = inspector;
            if (nullable) {
                isPresentOutStream = streamFactory.createStream(id,
                        OrcProto.Stream.Kind.PRESENT);
                isPresent = new BitFieldWriter(isPresentOutStream, 1);
            } else {
                isPresent = null;
            }
            this.foundNulls = false;
            createBloomFilter = streamFactory.getBloomFilterColumns()[columnId];
            indexStatistics = ColumnStatisticsImpl.create(inspector);
            stripeColStatistics = ColumnStatisticsImpl.create(inspector);
            fileStatistics = ColumnStatisticsImpl.create(inspector);
            childrenWriters = new TreeWriter[0];
            rowIndex = OrcProto.RowIndex.newBuilder();
            rowIndexEntry = OrcProto.RowIndexEntry.newBuilder();
            rowIndexPosition = new RowIndexPositionRecorder(rowIndexEntry);
            stripeStatsBuilders = Lists.newArrayList();
            if (streamFactory.buildIndex()) {
                rowIndexStream = streamFactory.createStream(id, OrcProto.Stream.Kind.ROW_INDEX);
            } else {
                rowIndexStream = null;
            }
            if (createBloomFilter) {
                bloomFilterEntry = OrcProto.BloomFilter.newBuilder();
                bloomFilterIndex = OrcProto.BloomFilterIndex.newBuilder();
                bloomFilterStream = streamFactory.createStream(id, OrcProto.Stream.Kind.BLOOM_FILTER);
                bloomFilter = new BloomFilterIO(streamFactory.getRowIndexStride(),
                        streamFactory.getBloomFilterFPP());
            } else {
                bloomFilterEntry = null;
                bloomFilterIndex = null;
                bloomFilterStream = null;
                bloomFilter = null;
            }
        }

        protected OrcProto.RowIndex.Builder getRowIndex() {
            return rowIndex;
        }

        protected ColumnStatisticsImpl getStripeStatistics() {
            return stripeColStatistics;
        }

        protected ColumnStatisticsImpl getFileStatistics() {
            return fileStatistics;
        }

        protected OrcProto.RowIndexEntry.Builder getRowIndexEntry() {
            return rowIndexEntry;
        }

        IntegerWriter createIntegerWriter(PositionedOutputStream output,
                                          boolean signed, boolean isDirectV2,
                                          StreamFactory writer) {
            if (isDirectV2) {
                boolean alignedBitpacking = false;
                if (writer.getEncodingStrategy().equals(EncodingStrategy.SPEED)) {
                    alignedBitpacking = true;
                }
                return new RunLengthIntegerWriterV2(output, signed, alignedBitpacking);
            } else {
                return new RunLengthIntegerWriter(output, signed);
            }
        }

        boolean isNewWriteFormat(StreamFactory writer) {
            return writer.getVersion() != OrcFile.Version.V_0_11;
        }

        /**
         * Add a new value to the column.
         *
         * @param obj The value to write to the column
         * @throws IOException if an error occurs during add
         */
        void write(Object obj) throws IOException {
            if (obj != null) {
                indexStatistics.increment();
            } else {
                indexStatistics.setNull();
            }
            if (isPresent != null) {
                isPresent.write(obj == null ? 0 : 1);
                if (obj == null) {
                    foundNulls = true;
                }
            }
        }

        private void removeIsPresentPositions() {
            for (int i = 0; i < rowIndex.getEntryCount(); ++i) {
                RowIndexEntry.Builder entry = rowIndex.getEntryBuilder(i);
                List<Long> positions = entry.getPositionsList();
                // bit streams use 3 positions if uncompressed, 4 if compressed
                positions = positions.subList(isCompressed ? 4 : 3, positions.size());
                entry.clearPositions();
                entry.addAllPositions(positions);
            }
        }

        /**
         * Write the stripe out to the file.
         *
         * @param builder              the stripe footer that contains the information about the
         *                             layout of the stripe. The TreeWriter is required to update
         *                             the footer with its information.
         * @param requiredIndexEntries the number of index entries that are
         *                             required. this is to check to make sure the
         *                             row index is well formed.
         * @throws IOException if an error occurs during write
         */
        void writeStripe(OrcProto.StripeFooter.Builder builder,
                         int requiredIndexEntries) throws IOException {
            if (isPresent != null) {
                isPresent.flush();

                // if no nulls are found in a stream, then suppress the stream
                if (!foundNulls) {
                    isPresentOutStream.suppress();
                    // since isPresent bitstream is suppressed, update the index to
                    // remove the positions of the isPresent stream
                    if (rowIndexStream != null) {
                        removeIsPresentPositions();
                    }
                }
            }

            // merge stripe-level column statistics to file statistics and write it to
            // stripe statistics
            OrcProto.StripeStatistics.Builder stripeStatsBuilder = OrcProto.StripeStatistics.newBuilder();
            writeStripeStatistics(stripeStatsBuilder, this);
            stripeStatsBuilders.add(stripeStatsBuilder);

            // reset the flag for next stripe
            foundNulls = false;

            builder.addColumns(getEncoding());
            builder.setWriterTimezone(TimeZone.getDefault().getID());
            if (rowIndexStream != null) {
                if (rowIndex.getEntryCount() != requiredIndexEntries) {
                    throw new IllegalArgumentException("Column has wrong number of " +
                            "index entries found: " + rowIndex.getEntryCount() + " expected: " +
                            requiredIndexEntries);
                }
                rowIndex.build().writeTo(rowIndexStream);
                rowIndexStream.flush();
            }
            rowIndex.clear();
            rowIndexEntry.clear();

            // write the bloom filter to out stream
            if (bloomFilterStream != null) {
                bloomFilterIndex.build().writeTo(bloomFilterStream);
                bloomFilterStream.flush();
                bloomFilterIndex.clear();
                bloomFilterEntry.clear();
            }
        }

        private void writeStripeStatistics(OrcProto.StripeStatistics.Builder builder,
                                           TreeWriter treeWriter) {
            treeWriter.fileStatistics.merge(treeWriter.stripeColStatistics);
            builder.addColStats(treeWriter.stripeColStatistics.serialize().build());
            treeWriter.stripeColStatistics.reset();
            for (TreeWriter child : treeWriter.getChildrenWriters()) {
                writeStripeStatistics(builder, child);
            }
        }

        TreeWriter[] getChildrenWriters() {
            return childrenWriters;
        }

        /**
         * Get the encoding for this column.
         *
         * @return the information about the encoding of this column
         */
        OrcProto.ColumnEncoding getEncoding() {
            return OrcProto.ColumnEncoding.newBuilder().setKind(
                    OrcProto.ColumnEncoding.Kind.DIRECT).build();
        }

        /**
         * Create a row index entry with the previous location and the current
         * index statistics. Also merges the index statistics into the file
         * statistics before they are cleared. Finally, it records the start of the
         * next index and ensures all of the children columns also create an entry.
         *
         * @throws IOException if an error occurs during create
         */
        void createRowIndexEntry() throws IOException {
            stripeColStatistics.merge(indexStatistics);
            rowIndexEntry.setStatistics(indexStatistics.serialize());
            indexStatistics.reset();
            rowIndex.addEntry(rowIndexEntry);
            rowIndexEntry.clear();
            addBloomFilterEntry();
            recordPosition(rowIndexPosition);
            for (TreeWriter child : childrenWriters) {
                child.createRowIndexEntry();
            }
        }

        void addBloomFilterEntry() {
            if (createBloomFilter) {
                bloomFilterEntry.setNumHashFunctions(bloomFilter.getNumHashFunctions());
                bloomFilterEntry.addAllBitset(Longs.asList(bloomFilter.getBitSet()));
                bloomFilterIndex.addBloomFilter(bloomFilterEntry.build());
                bloomFilter.reset();
                bloomFilterEntry.clear();
            }
        }

        /**
         * Record the current position in each of this column's streams.
         *
         * @param recorder where should the locations be recorded
         * @throws IOException if an error occurs during position recording
         */
        void recordPosition(PositionRecorder recorder) throws IOException {
            if (isPresent != null) {
                isPresent.getPosition(recorder);
            }
        }

        /**
         * Estimate how much memory the writer is consuming excluding the streams.
         *
         * @return the number of bytes.
         */
        long estimateMemory() {
            long result = 0;
            for (TreeWriter child : childrenWriters) {
                result += child.estimateMemory();
            }
            return result;
        }

        /**
         * Handle the top level object write.
         *
         * This default method is used for all types except structs, which are the
         * typical case. VectorizedRowBatch assumes the top level object is a
         * struct, so we use the first column for all other types.
         * @param batch the batch to write from
         * @param offset the row to start on
         * @param length the number of rows to write
         * @throws IOException if an error occurs during write
         */
        void writeRootBatch(VectorizedRowBatch batch, int offset,
                            int length) throws IOException {
            writeBatch(batch.cols[0], offset, length);
        }

        /**
         * Write the values from the given vector from offset for length elements.
         * @param vector the vector to write from
         * @param offset the first value from the vector to write
         * @param length the number of values from the vector to write
         * @throws IOException if an error occurs during write
         */
        void writeBatch(ColumnVector vector, int offset,
                        int length) throws IOException {
            if (vector.noNulls) {
                if (isPresent != null) {
                    for (int i = 0; i < length; ++i) {
                        isPresent.write(1);
                        indexStatistics.increment();
                    }
                }
            } else {
                if (vector.isRepeating) {
                    boolean isNull = vector.isNull[0];
                    if (isPresent != null) {
                        for (int i = 0; i < length; ++i) {
                            isPresent.write(isNull ? 0 : 1);
                        }
                    }
                    if (isNull) {
                        foundNulls = true;
                        indexStatistics.setNull();
                    } else {
                        indexStatistics.increment();
                    }
                } else {
                    // count the number of non-null values
                    int nonNullCount = 0;
                    for(int i = 0; i < length; ++i) {
                        boolean isNull = vector.isNull[i + offset];
                        if (!isNull) {
                            nonNullCount += 1;
                        }
                        if (isPresent != null) {
                            isPresent.write(isNull ? 0 : 1);
                        }
                    }
                    indexStatistics.increment();
                    if (nonNullCount != length) {
                        foundNulls = true;
                        indexStatistics.setNull();
                    }
                }
            }
        }
    }

    private static class BooleanTreeWriter extends TreeWriter {
        private final BitFieldWriter writer;

        BooleanTreeWriter(int columnId,
                          ObjectInspector inspector,
                          StreamFactory writer,
                          boolean nullable) throws IOException {
            super(columnId, inspector, writer, nullable);
            PositionedOutputStream out = writer.createStream(id,
                    OrcProto.Stream.Kind.DATA);
            this.writer = new BitFieldWriter(out, 1);
            recordPosition(rowIndexPosition);
        }

        @Override
        void write(Object obj) throws IOException {
            super.write(obj);
            if (obj != null) {
                boolean val = ((BooleanObjectInspector) inspector).get(obj);
                indexStatistics.updateBoolean(val);
                writer.write(val ? 1 : 0);
            }
        }

        @Override
        void writeStripe(OrcProto.StripeFooter.Builder builder,
                         int requiredIndexEntries) throws IOException {
            super.writeStripe(builder, requiredIndexEntries);
            writer.flush();
            recordPosition(rowIndexPosition);
        }

        @Override
        void recordPosition(PositionRecorder recorder) throws IOException {
            super.recordPosition(recorder);
            writer.getPosition(recorder);
        }
    }

    private static class ByteTreeWriter extends TreeWriter {
        private final RunLengthByteWriter writer;

        ByteTreeWriter(int columnId,
                       ObjectInspector inspector,
                       StreamFactory writer,
                       boolean nullable) throws IOException {
            super(columnId, inspector, writer, nullable);
            this.writer = new RunLengthByteWriter(writer.createStream(id,
                    OrcProto.Stream.Kind.DATA));
            recordPosition(rowIndexPosition);
        }

        @Override
        void write(Object obj) throws IOException {
            super.write(obj);
            if (obj != null) {
                byte val = ((ByteObjectInspector) inspector).get(obj);
                indexStatistics.updateInteger(val);
                if (createBloomFilter) {
                    bloomFilter.addLong(val);
                }
                writer.write(val);
            }
        }

        @Override
        void writeStripe(OrcProto.StripeFooter.Builder builder,
                         int requiredIndexEntries) throws IOException {
            super.writeStripe(builder, requiredIndexEntries);
            writer.flush();
            recordPosition(rowIndexPosition);
        }

        @Override
        void recordPosition(PositionRecorder recorder) throws IOException {
            super.recordPosition(recorder);
            writer.getPosition(recorder);
        }
    }

    private static class IntegerTreeWriter extends TreeWriter {
        private final IntegerWriter writer;
        private final ShortObjectInspector shortInspector;
        private final IntObjectInspector intInspector;
        private final LongObjectInspector longInspector;
        private boolean isDirectV2 = true;

        IntegerTreeWriter(int columnId,
                          ObjectInspector inspector,
                          StreamFactory writer,
                          boolean nullable) throws IOException {
            super(columnId, inspector, writer, nullable);
            OutStream out = writer.createStream(id,
                    OrcProto.Stream.Kind.DATA);
            this.isDirectV2 = isNewWriteFormat(writer);
            this.writer = createIntegerWriter(out, true, isDirectV2, writer);
            if (inspector instanceof IntObjectInspector) {
                intInspector = (IntObjectInspector) inspector;
                shortInspector = null;
                longInspector = null;
            } else {
                intInspector = null;
                if (inspector instanceof LongObjectInspector) {
                    longInspector = (LongObjectInspector) inspector;
                    shortInspector = null;
                } else {
                    shortInspector = (ShortObjectInspector) inspector;
                    longInspector = null;
                }
            }
            recordPosition(rowIndexPosition);
        }

        @Override
        OrcProto.ColumnEncoding getEncoding() {
            if (isDirectV2) {
                return OrcProto.ColumnEncoding.newBuilder()
                        .setKind(OrcProto.ColumnEncoding.Kind.DIRECT_V2).build();
            }
            return OrcProto.ColumnEncoding.newBuilder()
                    .setKind(OrcProto.ColumnEncoding.Kind.DIRECT).build();
        }

        @Override
        void write(Object obj) throws IOException {
            super.write(obj);
            if (obj != null) {
                long val;
                if (intInspector != null) {
                    val = intInspector.get(obj);
                } else if (longInspector != null) {
                    val = longInspector.get(obj);
                } else {
                    val = shortInspector.get(obj);
                }
                indexStatistics.updateInteger(val);
                if (createBloomFilter) {
                    // integers are converted to longs in column statistics and during SARG evaluation
                    bloomFilter.addLong(val);
                }
                writer.write(val);
            }
        }

        @Override
        void writeStripe(OrcProto.StripeFooter.Builder builder,
                         int requiredIndexEntries) throws IOException {
            super.writeStripe(builder, requiredIndexEntries);
            writer.flush();
            recordPosition(rowIndexPosition);
        }

        @Override
        void recordPosition(PositionRecorder recorder) throws IOException {
            super.recordPosition(recorder);
            writer.getPosition(recorder);
        }
    }

    private static class FloatTreeWriter extends TreeWriter {
        private final PositionedOutputStream stream;
        private final SerializationUtils utils;

        FloatTreeWriter(int columnId,
                        ObjectInspector inspector,
                        StreamFactory writer,
                        boolean nullable) throws IOException {
            super(columnId, inspector, writer, nullable);
            this.stream = writer.createStream(id,
                    OrcProto.Stream.Kind.DATA);
            this.utils = new SerializationUtils();
            recordPosition(rowIndexPosition);
        }

        @Override
        void write(Object obj) throws IOException {
            super.write(obj);
            if (obj != null) {
                float val = ((FloatObjectInspector) inspector).get(obj);
                indexStatistics.updateDouble(val);
                if (createBloomFilter) {
                    // floats are converted to doubles in column statistics and during SARG evaluation
                    bloomFilter.addDouble(val);
                }
                utils.writeFloat(stream, val);
            }
        }

        @Override
        void writeStripe(OrcProto.StripeFooter.Builder builder,
                         int requiredIndexEntries) throws IOException {
            super.writeStripe(builder, requiredIndexEntries);
            stream.flush();
            recordPosition(rowIndexPosition);
        }

        @Override
        void recordPosition(PositionRecorder recorder) throws IOException {
            super.recordPosition(recorder);
            stream.getPosition(recorder);
        }
    }

    private static class DoubleTreeWriter extends TreeWriter {
        private final PositionedOutputStream stream;
        private final SerializationUtils utils;

        DoubleTreeWriter(int columnId,
                         ObjectInspector inspector,
                         StreamFactory writer,
                         boolean nullable) throws IOException {
            super(columnId, inspector, writer, nullable);
            this.stream = writer.createStream(id,
                    OrcProto.Stream.Kind.DATA);
            this.utils = new SerializationUtils();
            recordPosition(rowIndexPosition);
        }

        @Override
        void write(Object obj) throws IOException {
            super.write(obj);
            if (obj != null) {
                double val = ((DoubleObjectInspector) inspector).get(obj);
                indexStatistics.updateDouble(val);
                if (createBloomFilter) {
                    bloomFilter.addDouble(val);
                }
                utils.writeDouble(stream, val);
            }
        }

        @Override
        void writeStripe(OrcProto.StripeFooter.Builder builder,
                         int requiredIndexEntries) throws IOException {
            super.writeStripe(builder, requiredIndexEntries);
            stream.flush();
            recordPosition(rowIndexPosition);
        }

        @Override
        void recordPosition(PositionRecorder recorder) throws IOException {
            super.recordPosition(recorder);
            stream.getPosition(recorder);
        }
    }

    private static class StringTreeWriter extends TreeWriter {
        private static final int INITIAL_DICTIONARY_SIZE = 4096;
        private final OutStream stringOutput;
        private final IntegerWriter lengthOutput;
        private final IntegerWriter rowOutput;
        private final StringRedBlackTree dictionary =
                new StringRedBlackTree(INITIAL_DICTIONARY_SIZE);
        private final DynamicIntArray rows = new DynamicIntArray();
        private final PositionedOutputStream directStreamOutput;
        private final IntegerWriter directLengthOutput;
        private final List<OrcProto.RowIndexEntry> savedRowIndex =
                new ArrayList<OrcProto.RowIndexEntry>();
        private final boolean buildIndex;
        private final List<Long> rowIndexValueCount = new ArrayList<Long>();
        // If the number of keys in a dictionary is greater than this fraction of
        //the total number of non-null rows, turn off dictionary encoding
        private final float dictionaryKeySizeThreshold;
        private boolean useDictionaryEncoding = true;
        private boolean isDirectV2 = true;
        private boolean doneDictionaryCheck;
        private final boolean strideDictionaryCheck;

        StringTreeWriter(int columnId,
                         ObjectInspector inspector,
                         StreamFactory writer,
                         boolean nullable) throws IOException {
            super(columnId, inspector, writer, nullable);
            this.isDirectV2 = isNewWriteFormat(writer);
            stringOutput = writer.createStream(id,
                    OrcProto.Stream.Kind.DICTIONARY_DATA);
            lengthOutput = createIntegerWriter(writer.createStream(id,
                    OrcProto.Stream.Kind.LENGTH), false, isDirectV2, writer);
            rowOutput = createIntegerWriter(writer.createStream(id,
                    OrcProto.Stream.Kind.DATA), false, isDirectV2, writer);
            recordPosition(rowIndexPosition);
            rowIndexValueCount.add(0L);
            buildIndex = writer.buildIndex();
            directStreamOutput = writer.createStream(id, OrcProto.Stream.Kind.DATA);
            directLengthOutput = createIntegerWriter(writer.createStream(id,
                    OrcProto.Stream.Kind.LENGTH), false, isDirectV2, writer);
            dictionaryKeySizeThreshold = writer.getConfiguration().getFloat(
                    HiveConf.ConfVars.HIVE_ORC_DICTIONARY_KEY_SIZE_THRESHOLD.varname,
                    HiveConf.ConfVars.HIVE_ORC_DICTIONARY_KEY_SIZE_THRESHOLD.defaultFloatVal);
            strideDictionaryCheck = writer.getConfiguration().getBoolean(
                    HiveConf.ConfVars.HIVE_ORC_ROW_INDEX_STRIDE_DICTIONARY_CHECK.varname,
                    HiveConf.ConfVars.HIVE_ORC_ROW_INDEX_STRIDE_DICTIONARY_CHECK.defaultBoolVal);
            doneDictionaryCheck = false;
        }

        /**
         * Method to retrieve text values from the value object, which can be overridden
         * by subclasses.
         *
         * @param obj value
         * @return Text text value from obj
         */
        Text getTextValue(Object obj) {
            return ((StringObjectInspector) inspector).getPrimitiveWritableObject(obj);
        }

        @Override
        void write(Object obj) throws IOException {
            super.write(obj);
            if (obj != null) {
                Text val = getTextValue(obj);
                if (useDictionaryEncoding || !strideDictionaryCheck) {
                    rows.add(dictionary.add(val));
                } else {
                    // write data and length
                    directStreamOutput.write(val.getBytes(), 0, val.getLength());
                    directLengthOutput.write(val.getLength());
                }
                indexStatistics.updateString(val);
                if (createBloomFilter) {
                    bloomFilter.addBytes(val.getBytes(), val.getLength());
                }
            }
        }

        private boolean checkDictionaryEncoding() {
            if (!doneDictionaryCheck) {
                // Set the flag indicating whether or not to use dictionary encoding
                // based on whether or not the fraction of distinct keys over number of
                // non-null rows is less than the configured threshold
                float ratio = rows.size() > 0 ? (float) (dictionary.size()) / rows.size() : 0.0f;
                useDictionaryEncoding = !isDirectV2 || ratio <= dictionaryKeySizeThreshold;
                doneDictionaryCheck = true;
            }
            return useDictionaryEncoding;
        }

        @Override
        void writeStripe(OrcProto.StripeFooter.Builder builder,
                         int requiredIndexEntries) throws IOException {
            // if rows in stripe is less than dictionaryCheckAfterRows, dictionary
            // checking would not have happened. So do it again here.
            checkDictionaryEncoding();

            if (useDictionaryEncoding) {
                flushDictionary();
            } else {
                // flushout any left over entries from dictionary
                if (rows.size() > 0) {
                    flushDictionary();
                }

                // suppress the stream for every stripe if dictionary is disabled
                stringOutput.suppress();
            }

            // we need to build the rowindex before calling super, since it
            // writes it out.
            super.writeStripe(builder, requiredIndexEntries);
            stringOutput.flush();
            lengthOutput.flush();
            rowOutput.flush();
            directStreamOutput.flush();
            directLengthOutput.flush();
            // reset all of the fields to be ready for the next stripe.
            dictionary.clear();
            savedRowIndex.clear();
            rowIndexValueCount.clear();
            recordPosition(rowIndexPosition);
            rowIndexValueCount.add(0L);

            if (!useDictionaryEncoding) {
                // record the start positions of first index stride of next stripe i.e
                // beginning of the direct streams when dictionary is disabled
                recordDirectStreamPosition();
            }
        }

        private void flushDictionary() throws IOException {
            final int[] dumpOrder = new int[dictionary.size()];

            if (useDictionaryEncoding) {
                // Write the dictionary by traversing the red-black tree writing out
                // the bytes and lengths; and creating the map from the original order
                // to the final sorted order.

                dictionary.visit(new StringRedBlackTree.Visitor() {
                    private int currentId = 0;

                    @Override
                    public void visit(StringRedBlackTree.VisitorContext context
                    ) throws IOException {
                        context.writeBytes(stringOutput);
                        lengthOutput.write(context.getLength());
                        dumpOrder[context.getOriginalPosition()] = currentId++;
                    }
                });
            } else {
                // for direct encoding, we don't want the dictionary data stream
                stringOutput.suppress();
            }
            int length = rows.size();
            int rowIndexEntry = 0;
            OrcProto.RowIndex.Builder rowIndex = getRowIndex();
            Text text = new Text();
            // write the values translated into the dump order.
            for (int i = 0; i <= length; ++i) {
                // now that we are writing out the row values, we can finalize the
                // row index
                if (buildIndex) {
                    while (i == rowIndexValueCount.get(rowIndexEntry) && rowIndexEntry < savedRowIndex.size()) {
                        OrcProto.RowIndexEntry.Builder base =
                                savedRowIndex.get(rowIndexEntry++).toBuilder();
                        if (useDictionaryEncoding) {
                            rowOutput.getPosition(new RowIndexPositionRecorder(base));
                        } else {
                            PositionRecorder posn = new RowIndexPositionRecorder(base);
                            directStreamOutput.getPosition(posn);
                            directLengthOutput.getPosition(posn);
                        }
                        rowIndex.addEntry(base.build());
                    }
                }
                if (i != length) {
                    if (useDictionaryEncoding) {
                        rowOutput.write(dumpOrder[rows.get(i)]);
                    } else {
                        dictionary.getText(text, rows.get(i));
                        directStreamOutput.write(text.getBytes(), 0, text.getLength());
                        directLengthOutput.write(text.getLength());
                    }
                }
            }
            rows.clear();
        }

        @Override
        OrcProto.ColumnEncoding getEncoding() {
            // Returns the encoding used for the last call to writeStripe
            if (useDictionaryEncoding) {
                if (isDirectV2) {
                    return OrcProto.ColumnEncoding.newBuilder().setKind(
                            OrcProto.ColumnEncoding.Kind.DICTIONARY_V2).setDictionarySize(dictionary.size()).build();
                }
                return OrcProto.ColumnEncoding.newBuilder().setKind(
                        OrcProto.ColumnEncoding.Kind.DICTIONARY).setDictionarySize(dictionary.size()).build();
            } else {
                if (isDirectV2) {
                    return OrcProto.ColumnEncoding.newBuilder().setKind(
                            OrcProto.ColumnEncoding.Kind.DIRECT_V2).build();
                }
                return OrcProto.ColumnEncoding.newBuilder().setKind(
                        OrcProto.ColumnEncoding.Kind.DIRECT).build();
            }
        }

        /**
         * This method doesn't call the super method, because unlike most of the
         * other TreeWriters, this one can't record the position in the streams
         * until the stripe is being flushed. Therefore it saves all of the entries
         * and augments them with the final information as the stripe is written.
         *
         * @throws IOException if an error occurs on create
         */
        @Override
        void createRowIndexEntry() throws IOException {
            getStripeStatistics().merge(indexStatistics);
            OrcProto.RowIndexEntry.Builder rowIndexEntry = getRowIndexEntry();
            rowIndexEntry.setStatistics(indexStatistics.serialize());
            indexStatistics.reset();
            OrcProto.RowIndexEntry base = rowIndexEntry.build();
            savedRowIndex.add(base);
            rowIndexEntry.clear();
            addBloomFilterEntry();
            recordPosition(rowIndexPosition);
            rowIndexValueCount.add(Long.valueOf(rows.size()));
            if (strideDictionaryCheck) {
                checkDictionaryEncoding();
            }
            if (!useDictionaryEncoding) {
                if (rows.size() > 0) {
                    flushDictionary();
                    // just record the start positions of next index stride
                    recordDirectStreamPosition();
                } else {
                    // record the start positions of next index stride
                    recordDirectStreamPosition();
                    getRowIndex().addEntry(base);
                }
            }
        }

        private void recordDirectStreamPosition() throws IOException {
            directStreamOutput.getPosition(rowIndexPosition);
            directLengthOutput.getPosition(rowIndexPosition);
        }

        @Override
        long estimateMemory() {
            return rows.getSizeInBytes() + dictionary.getSizeInBytes();
        }
    }

    /**
     * Under the covers, char is written to ORC the same way as string.
     */
    private static class CharTreeWriter extends StringTreeWriter {

        CharTreeWriter(int columnId,
                       ObjectInspector inspector,
                       StreamFactory writer,
                       boolean nullable) throws IOException {
            super(columnId, inspector, writer, nullable);
        }

        /**
         * Override base class implementation to support char values.
         */
        @Override
        Text getTextValue(Object obj) {
            return (((HiveCharObjectInspector) inspector)
                    .getPrimitiveWritableObject(obj)).getTextValue();
        }
    }

    /**
     * Under the covers, varchar is written to ORC the same way as string.
     */
    private static class VarcharTreeWriter extends StringTreeWriter {

        VarcharTreeWriter(int columnId,
                          ObjectInspector inspector,
                          StreamFactory writer,
                          boolean nullable) throws IOException {
            super(columnId, inspector, writer, nullable);
        }

        /**
         * Override base class implementation to support varchar values.
         */
        @Override
        Text getTextValue(Object obj) {
            return (((HiveVarcharObjectInspector) inspector)
                    .getPrimitiveWritableObject(obj)).getTextValue();
        }
    }

    private static class BinaryTreeWriter extends TreeWriter {
        private final PositionedOutputStream stream;
        private final IntegerWriter length;
        private boolean isDirectV2 = true;

        BinaryTreeWriter(int columnId,
                         ObjectInspector inspector,
                         StreamFactory writer,
                         boolean nullable) throws IOException {
            super(columnId, inspector, writer, nullable);
            this.stream = writer.createStream(id,
                    OrcProto.Stream.Kind.DATA);
            this.isDirectV2 = isNewWriteFormat(writer);
            this.length = createIntegerWriter(writer.createStream(id,
                    OrcProto.Stream.Kind.LENGTH), false, isDirectV2, writer);
            recordPosition(rowIndexPosition);
        }

        @Override
        OrcProto.ColumnEncoding getEncoding() {
            if (isDirectV2) {
                return OrcProto.ColumnEncoding.newBuilder()
                        .setKind(OrcProto.ColumnEncoding.Kind.DIRECT_V2).build();
            }
            return OrcProto.ColumnEncoding.newBuilder()
                    .setKind(OrcProto.ColumnEncoding.Kind.DIRECT).build();
        }

        @Override
        void write(Object obj) throws IOException {
            super.write(obj);
            if (obj != null) {
                BytesWritable val =
                        ((BinaryObjectInspector) inspector).getPrimitiveWritableObject(obj);
                stream.write(val.getBytes(), 0, val.getLength());
                length.write(val.getLength());
                indexStatistics.updateBinary(val);
                if (createBloomFilter) {
                    bloomFilter.addBytes(val.getBytes(), val.getLength());
                }
            }
        }

        @Override
        void writeStripe(OrcProto.StripeFooter.Builder builder,
                         int requiredIndexEntries) throws IOException {
            super.writeStripe(builder, requiredIndexEntries);
            stream.flush();
            length.flush();
            recordPosition(rowIndexPosition);
        }

        @Override
        void recordPosition(PositionRecorder recorder) throws IOException {
            super.recordPosition(recorder);
            stream.getPosition(recorder);
            length.getPosition(recorder);
        }
    }

    static final int MILLIS_PER_SECOND = 1000;
    static final String BASE_TIMESTAMP_STRING = "2015-01-01 00:00:00";

    private static class TimestampTreeWriter extends TreeWriter {
        private final IntegerWriter seconds;
        private final IntegerWriter nanos;
        private final boolean isDirectV2;
        private final long base_timestamp;

        TimestampTreeWriter(int columnId,
                            ObjectInspector inspector,
                            StreamFactory writer,
                            boolean nullable) throws IOException {
            super(columnId, inspector, writer, nullable);
            this.isDirectV2 = isNewWriteFormat(writer);
            this.seconds = createIntegerWriter(writer.createStream(id,
                    OrcProto.Stream.Kind.DATA), true, isDirectV2, writer);
            this.nanos = createIntegerWriter(writer.createStream(id,
                    OrcProto.Stream.Kind.SECONDARY), false, isDirectV2, writer);
            recordPosition(rowIndexPosition);
            // for unit tests to set different time zones
            this.base_timestamp = Timestamp.valueOf(BASE_TIMESTAMP_STRING).getTime() / MILLIS_PER_SECOND;
        }

        @Override
        OrcProto.ColumnEncoding getEncoding() {
            if (isDirectV2) {
                return OrcProto.ColumnEncoding.newBuilder()
                        .setKind(OrcProto.ColumnEncoding.Kind.DIRECT_V2).build();
            }
            return OrcProto.ColumnEncoding.newBuilder()
                    .setKind(OrcProto.ColumnEncoding.Kind.DIRECT).build();
        }

        @Override
        void write(Object obj) throws IOException {
            super.write(obj);
            if (obj != null) {
                Timestamp val =
                        ((TimestampObjectInspector) inspector).getPrimitiveJavaObject(obj);
                indexStatistics.updateTimestamp(val);
                seconds.write((val.getTime() / MILLIS_PER_SECOND) - base_timestamp);
                nanos.write(formatNanos(val.getNanos()));
                if (createBloomFilter) {
                    bloomFilter.addLong(val.getTime());
                }
            }
        }

        @Override
        void writeStripe(OrcProto.StripeFooter.Builder builder,
                         int requiredIndexEntries) throws IOException {
            super.writeStripe(builder, requiredIndexEntries);
            seconds.flush();
            nanos.flush();
            recordPosition(rowIndexPosition);
        }

        private static long formatNanos(int nanos) {
            if (nanos == 0) {
                return 0;
            } else if (nanos % 100 != 0) {
                return ((long) nanos) << 3;
            } else {
                nanos /= 100;
                int trailingZeros = 1;
                while (nanos % 10 == 0 && trailingZeros < 7) {
                    nanos /= 10;
                    trailingZeros += 1;
                }
                return ((long) nanos) << 3 | trailingZeros;
            }
        }

        @Override
        void recordPosition(PositionRecorder recorder) throws IOException {
            super.recordPosition(recorder);
            seconds.getPosition(recorder);
            nanos.getPosition(recorder);
        }
    }

    private static class DateTreeWriter extends TreeWriter {
        private final IntegerWriter writer;
        private final boolean isDirectV2;

        DateTreeWriter(int columnId,
                       ObjectInspector inspector,
                       StreamFactory writer,
                       boolean nullable) throws IOException {
            super(columnId, inspector, writer, nullable);
            OutStream out = writer.createStream(id,
                    OrcProto.Stream.Kind.DATA);
            this.isDirectV2 = isNewWriteFormat(writer);
            this.writer = createIntegerWriter(out, true, isDirectV2, writer);
            recordPosition(rowIndexPosition);
        }

        @Override
        void write(Object obj) throws IOException {
            super.write(obj);
            if (obj != null) {
                // Using the Writable here as it's used directly for writing as well as for stats.
                DateWritable val = ((DateObjectInspector) inspector).getPrimitiveWritableObject(obj);
                indexStatistics.updateDate(val);
                writer.write(val.getDays());
                if (createBloomFilter) {
                    bloomFilter.addLong(val.getDays());
                }
            }
        }

        @Override
        void writeStripe(OrcProto.StripeFooter.Builder builder,
                         int requiredIndexEntries) throws IOException {
            super.writeStripe(builder, requiredIndexEntries);
            writer.flush();
            recordPosition(rowIndexPosition);
        }

        @Override
        void recordPosition(PositionRecorder recorder) throws IOException {
            super.recordPosition(recorder);
            writer.getPosition(recorder);
        }

        @Override
        OrcProto.ColumnEncoding getEncoding() {
            if (isDirectV2) {
                return OrcProto.ColumnEncoding.newBuilder()
                        .setKind(OrcProto.ColumnEncoding.Kind.DIRECT_V2).build();
            }
            return OrcProto.ColumnEncoding.newBuilder()
                    .setKind(OrcProto.ColumnEncoding.Kind.DIRECT).build();
        }
    }

    private static class DecimalTreeWriter extends TreeWriter {
        private final PositionedOutputStream valueStream;
        private final IntegerWriter scaleStream;
        private final boolean isDirectV2;

        DecimalTreeWriter(int columnId,
                          ObjectInspector inspector,
                          StreamFactory writer,
                          boolean nullable) throws IOException {
            super(columnId, inspector, writer, nullable);
            this.isDirectV2 = isNewWriteFormat(writer);
            valueStream = writer.createStream(id, OrcProto.Stream.Kind.DATA);
            this.scaleStream = createIntegerWriter(writer.createStream(id,
                    OrcProto.Stream.Kind.SECONDARY), true, isDirectV2, writer);
            recordPosition(rowIndexPosition);
        }

        @Override
        OrcProto.ColumnEncoding getEncoding() {
            if (isDirectV2) {
                return OrcProto.ColumnEncoding.newBuilder()
                        .setKind(OrcProto.ColumnEncoding.Kind.DIRECT_V2).build();
            }
            return OrcProto.ColumnEncoding.newBuilder()
                    .setKind(OrcProto.ColumnEncoding.Kind.DIRECT).build();
        }

        @Override
        void write(Object obj) throws IOException {
            super.write(obj);
            if (obj != null) {
                HiveDecimal decimal = ((HiveDecimalObjectInspector) inspector).getPrimitiveJavaObject(obj);
                if (decimal == null) {
                    return;
                }
                SerializationUtils.writeBigInteger(valueStream,
                        decimal.unscaledValue());
                scaleStream.write(decimal.scale());
                indexStatistics.updateDecimal(decimal);
                if (createBloomFilter) {
                    bloomFilter.addString(decimal.toString());
                }
            }
        }

        @Override
        void writeStripe(OrcProto.StripeFooter.Builder builder,
                         int requiredIndexEntries) throws IOException {
            super.writeStripe(builder, requiredIndexEntries);
            valueStream.flush();
            scaleStream.flush();
            recordPosition(rowIndexPosition);
        }

        @Override
        void recordPosition(PositionRecorder recorder) throws IOException {
            super.recordPosition(recorder);
            valueStream.getPosition(recorder);
            scaleStream.getPosition(recorder);
        }
    }

    private static class StructTreeWriter extends TreeWriter {
        private final List<? extends StructField> fields;

        StructTreeWriter(int columnId,
                         ObjectInspector inspector,
                         StreamFactory writer,
                         boolean nullable) throws IOException {
            super(columnId, inspector, writer, nullable);
            StructObjectInspector structObjectInspector =
                    (StructObjectInspector) inspector;
            fields = structObjectInspector.getAllStructFieldRefs();
            childrenWriters = new TreeWriter[fields.size()];
            for (int i = 0; i < childrenWriters.length; ++i) {
                childrenWriters[i] = createTreeWriter(
                        fields.get(i).getFieldObjectInspector(), writer, true);
            }
            recordPosition(rowIndexPosition);
        }

        @Override
        void write(Object obj) throws IOException {
            super.write(obj);
            if (obj != null) {
                StructObjectInspector insp = (StructObjectInspector) inspector;
                for (int i = 0; i < fields.size(); ++i) {
                    StructField field = fields.get(i);
                    TreeWriter writer = childrenWriters[i];
                    writer.write(insp.getStructFieldData(obj, field));
                }
            }
        }

        @Override
        void writeStripe(OrcProto.StripeFooter.Builder builder,
                         int requiredIndexEntries) throws IOException {
            super.writeStripe(builder, requiredIndexEntries);
            for (TreeWriter child : childrenWriters) {
                child.writeStripe(builder, requiredIndexEntries);
            }
            recordPosition(rowIndexPosition);
        }
    }

    private static class ListTreeWriter extends TreeWriter {
        private final IntegerWriter lengths;
        private final boolean isDirectV2;

        ListTreeWriter(int columnId,
                       ObjectInspector inspector,
                       StreamFactory writer,
                       boolean nullable) throws IOException {
            super(columnId, inspector, writer, nullable);
            this.isDirectV2 = isNewWriteFormat(writer);
            ListObjectInspector listObjectInspector = (ListObjectInspector) inspector;
            childrenWriters = new TreeWriter[1];
            childrenWriters[0] =
                    createTreeWriter(listObjectInspector.getListElementObjectInspector(),
                            writer, true);
            lengths = createIntegerWriter(writer.createStream(columnId,
                    OrcProto.Stream.Kind.LENGTH), false, isDirectV2, writer);
            recordPosition(rowIndexPosition);
        }

        @Override
        OrcProto.ColumnEncoding getEncoding() {
            if (isDirectV2) {
                return OrcProto.ColumnEncoding.newBuilder()
                        .setKind(OrcProto.ColumnEncoding.Kind.DIRECT_V2).build();
            }
            return OrcProto.ColumnEncoding.newBuilder()
                    .setKind(OrcProto.ColumnEncoding.Kind.DIRECT).build();
        }

        @Override
        void write(Object obj) throws IOException {
            super.write(obj);
            if (obj != null) {
                ListObjectInspector insp = (ListObjectInspector) inspector;
                int len = insp.getListLength(obj);
                lengths.write(len);
                if (createBloomFilter) {
                    bloomFilter.addLong(len);
                }
                for (int i = 0; i < len; ++i) {
                    childrenWriters[0].write(insp.getListElement(obj, i));
                }
            }
        }

        @Override
        void writeStripe(OrcProto.StripeFooter.Builder builder,
                         int requiredIndexEntries) throws IOException {
            super.writeStripe(builder, requiredIndexEntries);
            lengths.flush();
            for (TreeWriter child : childrenWriters) {
                child.writeStripe(builder, requiredIndexEntries);
            }
            recordPosition(rowIndexPosition);
        }

        @Override
        void recordPosition(PositionRecorder recorder) throws IOException {
            super.recordPosition(recorder);
            lengths.getPosition(recorder);
        }
    }

    private static class MapTreeWriter extends TreeWriter {
        private final IntegerWriter lengths;
        private final boolean isDirectV2;

        MapTreeWriter(int columnId,
                      ObjectInspector inspector,
                      StreamFactory writer,
                      boolean nullable) throws IOException {
            super(columnId, inspector, writer, nullable);
            this.isDirectV2 = isNewWriteFormat(writer);
            MapObjectInspector insp = (MapObjectInspector) inspector;
            childrenWriters = new TreeWriter[2];
            childrenWriters[0] =
                    createTreeWriter(insp.getMapKeyObjectInspector(), writer, true);
            childrenWriters[1] =
                    createTreeWriter(insp.getMapValueObjectInspector(), writer, true);
            lengths = createIntegerWriter(writer.createStream(columnId,
                    OrcProto.Stream.Kind.LENGTH), false, isDirectV2, writer);
            recordPosition(rowIndexPosition);
        }

        @Override
        OrcProto.ColumnEncoding getEncoding() {
            if (isDirectV2) {
                return OrcProto.ColumnEncoding.newBuilder()
                        .setKind(OrcProto.ColumnEncoding.Kind.DIRECT_V2).build();
            }
            return OrcProto.ColumnEncoding.newBuilder()
                    .setKind(OrcProto.ColumnEncoding.Kind.DIRECT).build();
        }

        @Override
        void write(Object obj) throws IOException {
            super.write(obj);
            if (obj != null) {
                MapObjectInspector insp = (MapObjectInspector) inspector;
                // this sucks, but it will have to do until we can get a better
                // accessor in the MapObjectInspector.
                Map<?, ?> valueMap = insp.getMap(obj);
                lengths.write(valueMap.size());
                if (createBloomFilter) {
                    bloomFilter.addLong(valueMap.size());
                }
                for (Map.Entry<?, ?> entry : valueMap.entrySet()) {
                    childrenWriters[0].write(entry.getKey());
                    childrenWriters[1].write(entry.getValue());
                }
            }
        }

        @Override
        void writeStripe(OrcProto.StripeFooter.Builder builder,
                         int requiredIndexEntries) throws IOException {
            super.writeStripe(builder, requiredIndexEntries);
            lengths.flush();
            for (TreeWriter child : childrenWriters) {
                child.writeStripe(builder, requiredIndexEntries);
            }
            recordPosition(rowIndexPosition);
        }

        @Override
        void recordPosition(PositionRecorder recorder) throws IOException {
            super.recordPosition(recorder);
            lengths.getPosition(recorder);
        }
    }

    private static class UnionTreeWriter extends TreeWriter {
        private final RunLengthByteWriter tags;

        UnionTreeWriter(int columnId,
                        ObjectInspector inspector,
                        StreamFactory writer,
                        boolean nullable) throws IOException {
            super(columnId, inspector, writer, nullable);
            UnionObjectInspector insp = (UnionObjectInspector) inspector;
            List<ObjectInspector> choices = insp.getObjectInspectors();
            childrenWriters = new TreeWriter[choices.size()];
            for (int i = 0; i < childrenWriters.length; ++i) {
                childrenWriters[i] = createTreeWriter(choices.get(i), writer, true);
            }
            tags =
                    new RunLengthByteWriter(writer.createStream(columnId,
                            OrcProto.Stream.Kind.DATA));
            recordPosition(rowIndexPosition);
        }

        @Override
        void write(Object obj) throws IOException {
            super.write(obj);
            if (obj != null) {
                UnionObjectInspector insp = (UnionObjectInspector) inspector;
                byte tag = insp.getTag(obj);
                tags.write(tag);
                if (createBloomFilter) {
                    bloomFilter.addLong(tag);
                }
                childrenWriters[tag].write(insp.getField(obj));
            }
        }

        @Override
        void writeStripe(OrcProto.StripeFooter.Builder builder,
                         int requiredIndexEntries) throws IOException {
            super.writeStripe(builder, requiredIndexEntries);
            tags.flush();
            for (TreeWriter child : childrenWriters) {
                child.writeStripe(builder, requiredIndexEntries);
            }
            recordPosition(rowIndexPosition);
        }

        @Override
        void recordPosition(PositionRecorder recorder) throws IOException {
            super.recordPosition(recorder);
            tags.getPosition(recorder);
        }
    }

    private static TreeWriter createTreeWriter(ObjectInspector inspector,
                                               StreamFactory streamFactory,
                                               boolean nullable) throws IOException {
        switch (inspector.getCategory()) {
            case PRIMITIVE:
                switch (((PrimitiveObjectInspector) inspector).getPrimitiveCategory()) {
                    case BOOLEAN:
                        return new BooleanTreeWriter(streamFactory.getNextColumnId(),
                                inspector, streamFactory, nullable);
                    case BYTE:
                        return new ByteTreeWriter(streamFactory.getNextColumnId(),
                                inspector, streamFactory, nullable);
                    case SHORT:
                    case INT:
                    case LONG:
                        return new IntegerTreeWriter(streamFactory.getNextColumnId(),
                                inspector, streamFactory, nullable);
                    case FLOAT:
                        return new FloatTreeWriter(streamFactory.getNextColumnId(),
                                inspector, streamFactory, nullable);
                    case DOUBLE:
                        return new DoubleTreeWriter(streamFactory.getNextColumnId(),
                                inspector, streamFactory, nullable);
                    case STRING:
                        return new StringTreeWriter(streamFactory.getNextColumnId(),
                                inspector, streamFactory, nullable);
                    case CHAR:
                        return new CharTreeWriter(streamFactory.getNextColumnId(),
                                inspector, streamFactory, nullable);
                    case VARCHAR:
                        return new VarcharTreeWriter(streamFactory.getNextColumnId(),
                                inspector, streamFactory, nullable);
                    case BINARY:
                        return new BinaryTreeWriter(streamFactory.getNextColumnId(),
                                inspector, streamFactory, nullable);
                    case TIMESTAMP:
                        return new TimestampTreeWriter(streamFactory.getNextColumnId(),
                                inspector, streamFactory, nullable);
                    case DATE:
                        return new DateTreeWriter(streamFactory.getNextColumnId(),
                                inspector, streamFactory, nullable);
                    case DECIMAL:
                        return new DecimalTreeWriter(streamFactory.getNextColumnId(),
                                inspector, streamFactory, nullable);
                    default:
                        throw new IllegalArgumentException("Bad primitive category " +
                                ((PrimitiveObjectInspector) inspector).getPrimitiveCategory());
                }
            case STRUCT:
                return new StructTreeWriter(streamFactory.getNextColumnId(), inspector,
                        streamFactory, nullable);
            case MAP:
                return new MapTreeWriter(streamFactory.getNextColumnId(), inspector,
                        streamFactory, nullable);
            case LIST:
                return new ListTreeWriter(streamFactory.getNextColumnId(), inspector,
                        streamFactory, nullable);
            case UNION:
                return new UnionTreeWriter(streamFactory.getNextColumnId(), inspector,
                        streamFactory, nullable);
            default:
                throw new IllegalArgumentException("Bad category: " +
                        inspector.getCategory());
        }
    }

    private static void writeTypes(OrcProto.Footer.Builder builder,
                                   TreeWriter treeWriter) {
        OrcProto.Type.Builder type = OrcProto.Type.newBuilder();
        switch (treeWriter.inspector.getCategory()) {
            case PRIMITIVE:
                switch (((PrimitiveObjectInspector) treeWriter.inspector).getPrimitiveCategory()) {
                    case BOOLEAN:
                        type.setKind(OrcProto.Type.Kind.BOOLEAN);
                        break;
                    case BYTE:
                        type.setKind(OrcProto.Type.Kind.BYTE);
                        break;
                    case SHORT:
                        type.setKind(OrcProto.Type.Kind.SHORT);
                        break;
                    case INT:
                        type.setKind(OrcProto.Type.Kind.INT);
                        break;
                    case LONG:
                        type.setKind(OrcProto.Type.Kind.LONG);
                        break;
                    case FLOAT:
                        type.setKind(OrcProto.Type.Kind.FLOAT);
                        break;
                    case DOUBLE:
                        type.setKind(OrcProto.Type.Kind.DOUBLE);
                        break;
                    case STRING:
                        type.setKind(OrcProto.Type.Kind.STRING);
                        break;
                    case CHAR:
                        // The char length needs to be written to file and should be available
                        // from the object inspector
                        CharTypeInfo charTypeInfo = (CharTypeInfo) ((PrimitiveObjectInspector) treeWriter.inspector).getTypeInfo();
                        type.setKind(Type.Kind.CHAR);
                        type.setMaximumLength(charTypeInfo.getLength());
                        break;
                    case VARCHAR:
                        // The varchar length needs to be written to file and should be available
                        // from the object inspector
                        VarcharTypeInfo typeInfo = (VarcharTypeInfo) ((PrimitiveObjectInspector) treeWriter.inspector).getTypeInfo();
                        type.setKind(Type.Kind.VARCHAR);
                        type.setMaximumLength(typeInfo.getLength());
                        break;
                    case BINARY:
                        type.setKind(OrcProto.Type.Kind.BINARY);
                        break;
                    case TIMESTAMP:
                        type.setKind(OrcProto.Type.Kind.TIMESTAMP);
                        break;
                    case DATE:
                        type.setKind(OrcProto.Type.Kind.DATE);
                        break;
                    case DECIMAL:
                        DecimalTypeInfo decTypeInfo = (DecimalTypeInfo) ((PrimitiveObjectInspector) treeWriter.inspector).getTypeInfo();
                        type.setKind(OrcProto.Type.Kind.DECIMAL);
                        type.setPrecision(decTypeInfo.precision());
                        type.setScale(decTypeInfo.scale());
                        break;
                    default:
                        throw new IllegalArgumentException("Unknown primitive category: "
                                + ((PrimitiveObjectInspector) treeWriter.inspector).getPrimitiveCategory());
                }
                break;
            case LIST:
                type.setKind(OrcProto.Type.Kind.LIST);
                type.addSubtypes(treeWriter.childrenWriters[0].id);
                break;
            case MAP:
                type.setKind(OrcProto.Type.Kind.MAP);
                type.addSubtypes(treeWriter.childrenWriters[0].id);
                type.addSubtypes(treeWriter.childrenWriters[1].id);
                break;
            case STRUCT:
                type.setKind(OrcProto.Type.Kind.STRUCT);
                for (TreeWriter child : treeWriter.childrenWriters) {
                    type.addSubtypes(child.id);
                }
                for (StructField field : ((StructTreeWriter) treeWriter).fields) {
                    type.addFieldNames(field.getFieldName());
                }
                break;
            case UNION:
                type.setKind(OrcProto.Type.Kind.UNION);
                for (TreeWriter child : treeWriter.childrenWriters) {
                    type.addSubtypes(child.id);
                }
                break;
            default:
                throw new IllegalArgumentException("Unknown category: " + treeWriter.inspector.getCategory());
        }
        builder.addTypes(type);
        for (TreeWriter child : treeWriter.childrenWriters) {
            writeTypes(builder, child);
        }
    }

    @VisibleForTesting
    public OutputStream getStream() throws IOException {
        if (rawWriter == null) {
            rawWriter = new ByteCountingOutputStream(flowFileOutputStream);
            rawWriter.write(OrcFile.MAGIC.getBytes());
            headerLength = rawWriter.getBytesWritten();
            writer = new OutStream("metadata", bufferSize, codec,
                    new DirectStream(rawWriter));
            protobufWriter = CodedOutputStream.newInstance(writer);
        }
        return rawWriter;
    }

    private void createRowIndexEntry() throws IOException {
        treeWriter.createRowIndexEntry();
        rowsInIndex = 0;
    }

    private void flushStripe() throws IOException {
        getStream();
        if (buildIndex && rowsInIndex != 0) {
            createRowIndexEntry();
        }
        if (rowsInStripe != 0) {
            if (callback != null) {
                callback.preStripeWrite(callbackContext);
            }
            // finalize the data for the stripe
            int requiredIndexEntries = rowIndexStride == 0 ? 0 :
                    (int) ((rowsInStripe + rowIndexStride - 1) / rowIndexStride);
            OrcProto.StripeFooter.Builder builder =
                    OrcProto.StripeFooter.newBuilder();
            treeWriter.writeStripe(builder, requiredIndexEntries);
            long indexSize = 0;
            long dataSize = 0;
            for (Map.Entry<StreamName, BufferedStream> pair : streams.entrySet()) {
                BufferedStream stream = pair.getValue();
                if (!stream.isSuppressed()) {
                    stream.flush();
                    StreamName name = pair.getKey();
                    long streamSize = pair.getValue().getOutputSize();
                    builder.addStreams(OrcProto.Stream.newBuilder()
                            .setColumn(name.getColumn())
                            .setKind(name.getKind())
                            .setLength(streamSize));
                    if (StreamName.Area.INDEX == name.getArea()) {
                        indexSize += streamSize;
                    } else {
                        dataSize += streamSize;
                    }
                }
            }
            OrcProto.StripeFooter footer = builder.build();

            // Do we need to pad the file so the stripe doesn't straddle a block
            // boundary?
            long start = rawWriter.getBytesWritten();
            final long currentStripeSize = indexSize + dataSize + footer.getSerializedSize();
            final long available = blockSize - (start % blockSize);
            final long overflow = currentStripeSize - adjustedStripeSize;
            final float availRatio = (float) available / (float) defaultStripeSize;

            if (availRatio > 0.0f && availRatio < 1.0f
                    && availRatio > paddingTolerance) {
                // adjust default stripe size to fit into remaining space, also adjust
                // the next stripe for correction based on the current stripe size
                // and user specified padding tolerance. Since stripe size can overflow
                // the default stripe size we should apply this correction to avoid
                // writing portion of last stripe to next hdfs block.
                float correction = overflow > 0 ? (float) overflow
                        / (float) adjustedStripeSize : 0.0f;

                // correction should not be greater than user specified padding
                // tolerance
                correction = correction > paddingTolerance ? paddingTolerance
                        : correction;

                // adjust next stripe size based on current stripe estimate correction
                adjustedStripeSize = (long) ((1.0f - correction) * (availRatio * defaultStripeSize));
            } else if (availRatio >= 1.0) {
                adjustedStripeSize = defaultStripeSize;
            }

            if (availRatio < paddingTolerance && addBlockPadding) {
                long padding = blockSize - (start % blockSize);
                byte[] pad = new byte[(int) Math.min(HDFS_BUFFER_SIZE, padding)];
                LOG.info(String.format("Padding ORC by %d bytes (<=  %.2f * %d)",
                        padding, availRatio, defaultStripeSize));
                start += padding;
                while (padding > 0) {
                    int writeLen = (int) Math.min(padding, pad.length);
                    rawWriter.write(pad, 0, writeLen);
                    padding -= writeLen;
                }
                adjustedStripeSize = defaultStripeSize;
            } else if (currentStripeSize < blockSize
                    && (start % blockSize) + currentStripeSize > blockSize) {
                // even if you don't pad, reset the default stripe size when crossing a
                // block boundary
                adjustedStripeSize = defaultStripeSize;
            }

            // write out the data streams
            for (Map.Entry<StreamName, BufferedStream> pair : streams.entrySet()) {
                BufferedStream stream = pair.getValue();
                if (!stream.isSuppressed()) {
                    stream.spillTo(rawWriter);
                }
                stream.clear();
            }
            footer.writeTo(protobufWriter);
            protobufWriter.flush();
            writer.flush();
            long footerLength = rawWriter.getBytesWritten() - start - dataSize - indexSize;
            OrcProto.StripeInformation dirEntry =
                    OrcProto.StripeInformation.newBuilder()
                            .setOffset(start)
                            .setNumberOfRows(rowsInStripe)
                            .setIndexLength(indexSize)
                            .setDataLength(dataSize)
                            .setFooterLength(footerLength).build();
            stripes.add(dirEntry);
            rowCount += rowsInStripe;
            rowsInStripe = 0;
        }
    }

    private long computeRawDataSize() {
        long result = 0;
        for (TreeWriter child : treeWriter.getChildrenWriters()) {
            result += getRawDataSizeFromInspectors(child, child.inspector);
        }
        return result;
    }

    private long getRawDataSizeFromInspectors(TreeWriter child, ObjectInspector oi) {
        long total = 0;
        switch (oi.getCategory()) {
            case PRIMITIVE:
                total += getRawDataSizeFromPrimitives(child, oi);
                break;
            case LIST:
            case MAP:
            case UNION:
            case STRUCT:
                for (TreeWriter tw : child.childrenWriters) {
                    total += getRawDataSizeFromInspectors(tw, tw.inspector);
                }
                break;
            default:
                LOG.debug("Unknown object inspector category.");
                break;
        }
        return total;
    }

    private long getRawDataSizeFromPrimitives(TreeWriter child, ObjectInspector oi) {
        long result = 0;
        long numVals = child.fileStatistics.getNumberOfValues();
        switch (((PrimitiveObjectInspector) oi).getPrimitiveCategory()) {
            case BOOLEAN:
            case BYTE:
            case SHORT:
            case INT:
            case FLOAT:
                return numVals * JavaDataModel.get().primitive1();
            case LONG:
            case DOUBLE:
                return numVals * JavaDataModel.get().primitive2();
            case STRING:
            case VARCHAR:
            case CHAR:
                // ORC strings are converted to java Strings. so use JavaDataModel to
                // compute the overall size of strings
                child = (StringTreeWriter) child;
                StringColumnStatistics scs = (StringColumnStatistics) child.fileStatistics;
                numVals = numVals == 0 ? 1 : numVals;
                int avgStringLen = (int) (scs.getSum() / numVals);
                return numVals * JavaDataModel.get().lengthForStringOfLength(avgStringLen);
            case DECIMAL:
                return numVals * JavaDataModel.get().lengthOfDecimal();
            case DATE:
                return numVals * JavaDataModel.get().lengthOfDate();
            case BINARY:
                // get total length of binary blob
                BinaryColumnStatistics bcs = (BinaryColumnStatistics) child.fileStatistics;
                return bcs.getSum();
            case TIMESTAMP:
                return numVals * JavaDataModel.get().lengthOfTimestamp();
            default:
                LOG.debug("Unknown primitive category.");
                break;
        }

        return result;
    }

    private OrcProto.CompressionKind writeCompressionKind(CompressionKind kind) {
        switch (kind) {
            case NONE:
                return OrcProto.CompressionKind.NONE;
            case ZLIB:
                return OrcProto.CompressionKind.ZLIB;
            case SNAPPY:
                return OrcProto.CompressionKind.SNAPPY;
            case LZO:
                return OrcProto.CompressionKind.LZO;
            default:
                throw new IllegalArgumentException("Unknown compression " + kind);
        }
    }

    private void writeFileStatistics(OrcProto.Footer.Builder builder,
                                     TreeWriter writer) throws IOException {
        builder.addStatistics(writer.fileStatistics.serialize());
        for (TreeWriter child : writer.getChildrenWriters()) {
            writeFileStatistics(builder, child);
        }
    }

    private int writeMetadata() throws IOException {
        getStream();
        OrcProto.Metadata.Builder builder = OrcProto.Metadata.newBuilder();
        for (OrcProto.StripeStatistics.Builder ssb : treeWriter.stripeStatsBuilders) {
            builder.addStripeStats(ssb.build());
        }

        long startPosn = rawWriter.getBytesWritten();
        OrcProto.Metadata metadata = builder.build();
        metadata.writeTo(protobufWriter);
        protobufWriter.flush();
        writer.flush();
        return (int) (rawWriter.getBytesWritten() - startPosn);
    }

    private int writeFooter(long bodyLength) throws IOException {
        getStream();
        OrcProto.Footer.Builder builder = OrcProto.Footer.newBuilder();
        builder.setContentLength(bodyLength);
        builder.setHeaderLength(headerLength);
        builder.setNumberOfRows(rowCount);
        builder.setRowIndexStride(rowIndexStride);
        // populate raw data size
        rawDataSize = computeRawDataSize();
        // serialize the types
        writeTypes(builder, treeWriter);
        // add the stripe information
        for (OrcProto.StripeInformation stripe : stripes) {
            builder.addStripes(stripe);
        }
        // add the column statistics
        writeFileStatistics(builder, treeWriter);
        // add all of the user metadata
        for (Map.Entry<String, ByteString> entry : userMetadata.entrySet()) {
            builder.addMetadata(OrcProto.UserMetadataItem.newBuilder()
                    .setName(entry.getKey()).setValue(entry.getValue()));
        }
        long startPosn = rawWriter.getBytesWritten();
        OrcProto.Footer footer = builder.build();
        footer.writeTo(protobufWriter);
        protobufWriter.flush();
        writer.flush();
        return (int) (rawWriter.getBytesWritten() - startPosn);
    }

    private int writePostScript(int footerLength, int metadataLength) throws IOException {
        OrcProto.PostScript.Builder builder =
                OrcProto.PostScript.newBuilder()
                        .setCompression(writeCompressionKind(compress))
                        .setFooterLength(footerLength)
                        .setMetadataLength(metadataLength)
                        .setMagic(OrcFile.MAGIC)
                        .addVersion(version.getMajor())
                        .addVersion(version.getMinor())
                        .setWriterVersion(OrcFile.WriterVersion.HIVE_8732.getId());
        if (compress != CompressionKind.NONE) {
            builder.setCompressionBlockSize(bufferSize);
        }
        OrcProto.PostScript ps = builder.build();
        // need to write this uncompressed
        long startPosn = rawWriter.getBytesWritten();
        ps.writeTo(rawWriter);
        long length = rawWriter.getBytesWritten() - startPosn;
        if (length > 255) {
            throw new IllegalArgumentException("PostScript too large at " + length);
        }
        return (int) length;
    }

    private long estimateStripeSize() {
        long result = 0;
        for (BufferedStream stream : streams.values()) {
            result += stream.getBufferSize();
        }
        result += treeWriter.estimateMemory();
        return result;
    }

    @Override
    public synchronized void addUserMetadata(String name, ByteBuffer value) {
        userMetadata.put(name, ByteString.copyFrom(value));
    }

    @Override
    public void addRow(Object row) throws IOException {
        synchronized (this) {
            treeWriter.write(row);
            rowsInStripe += 1;
            if (buildIndex) {
                rowsInIndex += 1;

                if (rowsInIndex >= rowIndexStride) {
                    createRowIndexEntry();
                }
            }
        }
        memoryManager.addedRow();
    }

    public void addRowBatch(VectorizedRowBatch batch) throws IOException {
        if (buildIndex) {
            // Batch the writes up to the rowIndexStride so that we can get the
            // right size indexes.
            int posn = 0;
            while (posn < batch.size) {
                int chunkSize = Math.min(batch.size - posn,
                        rowIndexStride - rowsInIndex);
                treeWriter.writeRootBatch(batch, posn, chunkSize);
                posn += chunkSize;
                rowsInIndex += chunkSize;
                rowsInStripe += chunkSize;
                if (rowsInIndex >= rowIndexStride) {
                    createRowIndexEntry();
                }
            }
        } else {
            rowsInStripe += batch.size;
            treeWriter.writeRootBatch(batch, 0, batch.size);
        }
        memoryManager.addedRow();
    }


    @Override
    public void close() throws IOException {
        if (callback != null) {
            callback.preFooterWrite(callbackContext);
        }
        // remove us from the memory manager so that we don't get any callbacks
        memoryManager.removeWriter(path);
        // actually close the file
        flushStripe();
        int metadataLength = writeMetadata();
        int footerLength = writeFooter(rawWriter.getBytesWritten() - metadataLength);
        rawWriter.write(writePostScript(footerLength, metadataLength));
        rawWriter.close();

    }

    /**
     * Raw data size will be compute when writing the file footer. Hence raw data
     * size value will be available only after closing the writer.
     */
    @Override
    public long getRawDataSize() {
        return rawDataSize;
    }

    /**
     * Row count gets updated when flushing the stripes. To get accurate row
     * count call this method after writer is closed.
     */
    @Override
    public long getNumberOfRows() {
        return rowCount;
    }

    @Override
    public long writeIntermediateFooter() throws IOException {
        // flush any buffered rows
        flushStripe();
        // write a footer
        if (stripesAtLastFlush != stripes.size()) {
            if (callback != null) {
                callback.preFooterWrite(callbackContext);
            }
            int metaLength = writeMetadata();
            int footLength = writeFooter(rawWriter.getBytesWritten() - metaLength);
            rawWriter.write(writePostScript(footLength, metaLength));
            stripesAtLastFlush = stripes.size();
            rawWriter.flush();
        }
        return rawWriter.getBytesWritten();
    }

    @Override
    public void appendStripe(byte[] stripe, int offset, int length,
                             StripeInformation stripeInfo,
                             OrcProto.StripeStatistics stripeStatistics) throws IOException {
        checkArgument(stripe != null, "Stripe must not be null");
        checkArgument(length <= stripe.length,
                "Specified length must not be greater specified array length");
        checkArgument(stripeInfo != null, "Stripe information must not be null");
        checkArgument(stripeStatistics != null,
                "Stripe statistics must not be null");

        getStream();
        long start = rawWriter.getBytesWritten();
        long stripeLen = length;
        long availBlockSpace = blockSize - (start % blockSize);

        // see if stripe can fit in the current hdfs block, else pad the remaining
        // space in the block
        if (stripeLen < blockSize && stripeLen > availBlockSpace && addBlockPadding) {
            byte[] pad = new byte[(int) Math.min(HDFS_BUFFER_SIZE, availBlockSpace)];
            LOG.info(String.format("Padding ORC by %d bytes while merging..",
                    availBlockSpace));
            start += availBlockSpace;
            while (availBlockSpace > 0) {
                int writeLen = (int) Math.min(availBlockSpace, pad.length);
                rawWriter.write(pad, 0, writeLen);
                availBlockSpace -= writeLen;
            }
        }

        rawWriter.write(stripe);
        rowsInStripe = stripeStatistics.getColStats(0).getNumberOfValues();
        rowCount += rowsInStripe;

        // since we have already written the stripe, just update stripe statistics
        treeWriter.stripeStatsBuilders.add(stripeStatistics.toBuilder());

        // update file level statistics
        updateFileStatistics(stripeStatistics);

        // update stripe information
        OrcProto.StripeInformation dirEntry = OrcProto.StripeInformation
                .newBuilder()
                .setOffset(start)
                .setNumberOfRows(rowsInStripe)
                .setIndexLength(stripeInfo.getIndexLength())
                .setDataLength(stripeInfo.getDataLength())
                .setFooterLength(stripeInfo.getFooterLength())
                .build();
        stripes.add(dirEntry);

        // reset it after writing the stripe
        rowsInStripe = 0;
    }

    private void updateFileStatistics(OrcProto.StripeStatistics stripeStatistics) {
        List<OrcProto.ColumnStatistics> cs = stripeStatistics.getColStatsList();
        List<TreeWriter> allWriters = getAllColumnTreeWriters(treeWriter);
        for (int i = 0; i < allWriters.size(); i++) {
            allWriters.get(i).fileStatistics.merge(ColumnStatisticsImpl.deserialize(cs.get(i)));
        }
    }

    private List<TreeWriter> getAllColumnTreeWriters(TreeWriter rootTreeWriter) {
        List<TreeWriter> result = Lists.newArrayList();
        getAllColumnTreeWritersImpl(rootTreeWriter, result);
        return result;
    }

    private void getAllColumnTreeWritersImpl(TreeWriter tw,
                                             List<TreeWriter> result) {
        result.add(tw);
        for (TreeWriter child : tw.childrenWriters) {
            getAllColumnTreeWritersImpl(child, result);
        }
    }

    @Override
    public void appendUserMetadata(List<UserMetadataItem> userMetadata) {
        if (userMetadata != null) {
            for (UserMetadataItem item : userMetadata) {
                this.userMetadata.put(item.getName(), item.getValue());
            }
        }
    }
}
