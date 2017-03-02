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
package org.apache.nifi.controller.repository.io;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class ArrayManagedOutputStream extends OutputStream {

    private final MemoryManager memoryManager;

    private final ReadWriteLock rwLock = new ReentrantReadWriteLock();
    private final Lock readLock = rwLock.readLock();
    private final Lock writeLock = rwLock.writeLock();

    private final List<byte[]> blocks = new ArrayList<>();
    private int currentIndex;
    private byte[] currentBlock;
    private long curSize;

    public ArrayManagedOutputStream(final MemoryManager memoryManager) {
        this.memoryManager = memoryManager;
    }

    @Override
    public void write(final byte[] b, int off, final int len) throws IOException {
        writeLock.lock();
        try {
            final int bytesFreeThisBlock = currentBlock == null ? 0 : currentBlock.length - currentIndex;
            if (bytesFreeThisBlock >= len) {
                System.arraycopy(b, off, currentBlock, currentIndex, len);
                currentIndex += len;
                curSize += len;

                return;
            }

            // Try to get all of the blocks needed
            final long bytesNeeded = len - bytesFreeThisBlock;
            int blocksNeeded = (int) (bytesNeeded / memoryManager.getBlockSize());
            if (blocksNeeded * memoryManager.getBlockSize() < bytesNeeded) {
                blocksNeeded++;
            }

            // get all of the blocks that we need
            final List<byte[]> newBlocks = new ArrayList<>(blocksNeeded);
            for (int i = 0; i < blocksNeeded; i++) {
                final byte[] newBlock = memoryManager.checkOut();
                if (newBlock == null) {
                    memoryManager.checkIn(newBlocks);
                    throw new IOException("No space left in Content Repository");
                }

                newBlocks.add(newBlock);
            }

            // we've successfully obtained the blocks needed. Copy the data.
            // first copy what we can to the current block
            long bytesCopied = 0;
            final int bytesForCur = currentBlock == null ? 0 : currentBlock.length - currentIndex;
            if (bytesForCur > 0) {
                System.arraycopy(b, off, currentBlock, currentIndex, bytesForCur);

                off += bytesForCur;
                bytesCopied += bytesForCur;
                currentBlock = null;
                currentIndex = 0;
            }

            // then copy to all new blocks
            for (final byte[] block : newBlocks) {
                final int bytesToCopy = (int) Math.min(len - bytesCopied, block.length);
                System.arraycopy(b, off, block, 0, bytesToCopy);
                currentIndex = bytesToCopy;
                currentBlock = block;
                off += bytesToCopy;
                bytesCopied += bytesToCopy;
            }

            curSize += len;
            blocks.addAll(newBlocks);
        } finally {
            writeLock.unlock();
        }
    }

    @Override
    public void write(final int b) throws IOException {
        final byte[] bytes = new byte[1];
        bytes[0] = (byte) (b & 0xFF);
        write(bytes);
    }

    public void destroy() {
        writeLock.lock();
        try {
            memoryManager.checkIn(blocks);
            blocks.clear();
            currentBlock = null;
            currentIndex = 0;
            curSize = 0L;
        } finally {
            writeLock.unlock();
        }
    }

    public long size() {
        readLock.lock();
        try {
            return curSize;
        } finally {
            readLock.unlock();
        }
    }

    public void reset() {
        destroy();
    }

    public void writeTo(final OutputStream out) throws IOException {
        readLock.lock();
        try {
            int i = 0;
            for (final byte[] block : blocks) {
                if (++i == blocks.size()) {
                    if (currentIndex > 0) {
                        out.write(block, 0, currentIndex);
                    }
                } else {
                    out.write(block);
                }
            }
        } finally {
            readLock.unlock();
        }
    }

    public int getBufferLength() {
        readLock.lock();
        try {
            if (currentBlock == null) {
                return 0;
            }
            // all blocks are same size
            return blocks.size() * currentBlock.length;
        } finally {
            readLock.unlock();
        }
    }

    public InputStream newInputStream() {
        final int blockSize;
        final long totalSize;

        readLock.lock();
        try {
            if (blocks.isEmpty()) {
                return new ByteArrayInputStream(new byte[0]);
            }

            blockSize = memoryManager.getBlockSize();
            totalSize = curSize;
        } finally {
            readLock.unlock();
        }

        return new InputStream() {
            int blockIndex = 0;
            int byteIndex = 0;

            long bytesRead = 0L;

            @Override
            public int read() throws IOException {
                readLock.lock();
                try {
                    if (bytesRead >= totalSize) {
                        return -1;
                    }

                    if (byteIndex >= blockSize) {
                        blockIndex++;
                        byteIndex = 0;
                    }

                    final byte[] buffer = blocks.get(blockIndex);
                    final int b = buffer[byteIndex++] & 0xFF;
                    bytesRead++;

                    return b;
                } finally {
                    readLock.unlock();
                }
            }

            @Override
            public int read(final byte[] b, final int off, final int len) throws IOException {
                readLock.lock();
                try {
                    if (bytesRead >= totalSize) {
                        return -1;
                    }

                    if (byteIndex >= blockSize) {
                        blockIndex++;
                        byteIndex = 0;
                    }

                    final byte[] buffer = blocks.get(blockIndex);
                    final long bytesUnread = totalSize - bytesRead;
                    final int bytesToCopy = (int) Math.min(bytesUnread, Math.min(len, buffer.length - byteIndex));

                    System.arraycopy(buffer, byteIndex, b, off, bytesToCopy);
                    byteIndex += bytesToCopy;
                    bytesRead += bytesToCopy;

                    return bytesToCopy;
                } finally {
                    readLock.unlock();
                }
            }
        };
    }
}
