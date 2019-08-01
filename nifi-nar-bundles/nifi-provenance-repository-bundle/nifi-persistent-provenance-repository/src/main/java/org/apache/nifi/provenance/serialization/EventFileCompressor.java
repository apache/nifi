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

package org.apache.nifi.provenance.serialization;

import org.apache.nifi.provenance.store.EventFileManager;
import org.apache.nifi.provenance.toc.StandardTocReader;
import org.apache.nifi.provenance.toc.StandardTocWriter;
import org.apache.nifi.provenance.toc.TocReader;
import org.apache.nifi.provenance.toc.TocUtil;
import org.apache.nifi.provenance.toc.TocWriter;
import org.apache.nifi.provenance.util.CloseableUtil;
import org.apache.nifi.stream.io.ByteCountingOutputStream;
import org.apache.nifi.stream.io.GZIPOutputStream;
import org.apache.nifi.stream.io.NonCloseableOutputStream;
import org.apache.nifi.stream.io.StreamUtils;
import org.apache.nifi.util.FormatUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.EOFException;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;

/**
 * <p>
 * This class is responsible for compressing Event Files as a background task. This is done as a background task instead of being
 * done inline because if compression is performed inline, whenever NiFi is restarted (especially if done so abruptly), it is very
 * possible that the GZIP stream will be corrupt. As a result, we would stand to lose some Provenance Events when NiFi is restarted.
 * In order to avoid that, we write data in an uncompressed format and then compress the data in the background. Once the data has
 * been compressed, this task will then remove the original, uncompressed file. If the file is being read by another thread, this
 * task will wait for the other thread to finish reading the data before deleting the file. This synchronization of the File is handled
 * via the {@link EventFileManager Event File Manager}.
 * </p>
 */
public class EventFileCompressor implements Runnable {
    private static final Logger logger = LoggerFactory.getLogger(EventFileCompressor.class);
    private final BlockingQueue<File> filesToCompress;
    private final EventFileManager eventFileManager;
    private volatile boolean shutdown = false;

    public EventFileCompressor(final BlockingQueue<File> filesToCompress, final EventFileManager eventFileManager) {
        this.filesToCompress = filesToCompress;
        this.eventFileManager = eventFileManager;
    }

    public void shutdown() {
        shutdown = true;
    }

    @Override
    public void run() {
        while (!shutdown) {
            File uncompressedEventFile = null;

            try {
                final long start = System.nanoTime();
                uncompressedEventFile = filesToCompress.poll(1, TimeUnit.SECONDS);
                if (uncompressedEventFile == null || shutdown) {
                    continue;
                }

                File outputFile;
                long bytesBefore;
                StandardTocReader tocReader;

                File tmpTocFile = null;
                eventFileManager.obtainReadLock(uncompressedEventFile);
                try {
                    StandardTocWriter tocWriter = null;

                    final File tocFile = TocUtil.getTocFile(uncompressedEventFile);
                    try {
                        tocReader = new StandardTocReader(tocFile);
                    } catch (final FileNotFoundException fnfe) {
                        logger.debug("Attempted to compress event file {} but the TOC file {} could not be found", uncompressedEventFile, tocFile);
                        continue;
                    } catch (final EOFException eof) {
                        logger.info("Attempted to compress event file {} but encountered unexpected End-of-File when reading TOC file {}; this typically happens as a result of the data aging off " +
                            "from the Provenance Repository before it is able to be compressed.", uncompressedEventFile, tocFile);
                        continue;
                    } catch (final IOException e) {
                        logger.error("Failed to read TOC File {}", tocFile, e);
                        continue;
                    }

                    bytesBefore = uncompressedEventFile.length();

                    try {
                        outputFile = new File(uncompressedEventFile.getParentFile(), uncompressedEventFile.getName() + ".gz");
                        try {
                            tmpTocFile = new File(tocFile.getParentFile(), tocFile.getName() + ".tmp");
                            tocWriter = new StandardTocWriter(tmpTocFile, true, false);
                            compress(uncompressedEventFile, tocReader, outputFile, tocWriter);
                            tocWriter.close();
                        } catch (final IOException ioe) {
                            logger.error("Failed to compress {} on rollover", uncompressedEventFile, ioe);
                        }
                    } finally {
                        CloseableUtil.closeQuietly(tocReader, tocWriter);
                    }
                } finally {
                    eventFileManager.releaseReadLock(uncompressedEventFile);
                }

                eventFileManager.obtainWriteLock(uncompressedEventFile);
                try {
                    // Attempt to delete the input file and associated toc file
                    if (uncompressedEventFile.delete()) {
                        if (tocReader != null) {
                            final File tocFile = tocReader.getFile();
                            if (!tocFile.delete()) {
                                logger.warn("Failed to delete {}; this file should be cleaned up manually", tocFile);
                            }

                            if (tmpTocFile != null) {
                                tmpTocFile.renameTo(tocFile);
                            }
                        }
                    } else {
                        logger.warn("Failed to delete {}; this file should be cleaned up manually", uncompressedEventFile);
                    }
                } finally {
                    eventFileManager.releaseWriteLock(uncompressedEventFile);
                }

                final long millis = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start);
                final long bytesAfter = outputFile.length();
                final double reduction = 100 * (1 - (double) bytesAfter / (double) bytesBefore);
                final String reductionTwoDecimals = String.format("%.2f", reduction);
                logger.debug("Successfully compressed Provenance Event File {} in {} millis from {} to {}, a reduction of {}%",
                    uncompressedEventFile, millis, FormatUtils.formatDataSize(bytesBefore), FormatUtils.formatDataSize(bytesAfter), reductionTwoDecimals);
            } catch (final InterruptedException e) {
                Thread.currentThread().interrupt();
                return;
            } catch (final Exception e) {
                logger.error("Failed to compress {}", uncompressedEventFile, e);
            }
        }
    }

    private static void compress(final File input, final TocReader tocReader, final File output, final TocWriter tocWriter) throws IOException {
        try (final InputStream fis = new FileInputStream(input);
            final OutputStream fos = new FileOutputStream(output);
            final ByteCountingOutputStream byteCountingOut = new ByteCountingOutputStream(fos)) {

            int blockIndex = 0;
            while (true) {
                // Determine the min and max byte ranges for the current block.
                final long blockStart = tocReader.getBlockOffset(blockIndex);
                if (blockStart == -1) {
                    break;
                }

                long blockEnd = tocReader.getBlockOffset(blockIndex + 1);
                if (blockEnd < 0) {
                    blockEnd = input.length();
                }

                final long firstEventId = tocReader.getFirstEventIdForBlock(blockIndex);
                final long blockStartOffset = byteCountingOut.getBytesWritten();

                try (final OutputStream ncos = new NonCloseableOutputStream(byteCountingOut);
                    final OutputStream gzipOut = new GZIPOutputStream(ncos, 1)) {
                    StreamUtils.copy(fis, gzipOut, blockEnd - blockStart);
                }

                tocWriter.addBlockOffset(blockStartOffset, firstEventId);
                blockIndex++;
            }
        }

        // Close the TOC Reader and TOC Writer
        CloseableUtil.closeQuietly(tocReader, tocWriter);
    }
}
