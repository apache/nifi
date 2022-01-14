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
package org.apache.nifi.pmem;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.attribute.PosixFilePermission;
import java.util.Objects;
import java.util.Set;

/**
 * Not thread-safe; up to one thread should modify this object concurrently,
 * or synchronize externally if you want to support multi-threading.
 */
public class PmemMappedFile implements Closeable {
    private static final long HUGE2M_MASK = -(1L << 21);

    private final String path;
    private final long address;
    private final long length;
    private final boolean isPmem;

    private boolean closed;

    private static native void init();

    private static native PmemMappedFile pmem_map_file_open(String path)
            throws IOException;

    private static native PmemMappedFile pmem_map_file_create_excl(
            String path, long length, int mode) throws IOException;

    private static native PmemMappedFile pmem_map_file_create(
            String path, long length, int mode) throws IOException;

    private static native void pmem_unmap(long address, long length)
            throws IOException;

    private static native boolean pmem_memcpy_nodrain(
            long address, byte[] src, int off, int len);

    private static native boolean pmem_memcpy_noflush(
            long address, byte[] src, int off, int len);

    private static native void pmem_flush(long address, long length);

    private static native void pmem_drain();

    private static native void pmem_msync(long address, long length)
            throws IOException;

    private native ByteBuffer newDirectByteBufferView(long address, int length);

    /* The static initializer to load and initialize our native library */
    static {
        try {
            /*
             * We cannot load our library included in Jar directly. Instead,
             * we output the library into a file in a temporary directory,
             * then load that file.
             */
            final Path tempDir = Files.createTempDirectory(null);
            final String libFullName = "lib" + PmemMappedFile.class.getSimpleName() + ".so";
            final String libResource = "/" + libFullName;
            final File libFile = new File(tempDir.toFile(), libFullName);
            try (final InputStream in = PmemMappedFile.class.getResourceAsStream(libResource)) {
                if (in == null) {
                    /* Resource not found (occurs when unit-testing) */
                    throw new IOException();
                }
                Files.copy(in, libFile.toPath());
            }
            System.load(libFile.getPath());
        } catch (IOException e) {
            /* Fallback for unit test */
            System.loadLibrary(PmemMappedFile.class.getSimpleName());
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

        init();
    }

    /* Called only by the native pmem_map_file method via JNI */
    private PmemMappedFile(String path, long address, long length, boolean isPmem) {
        this.path = path;
        this.address = address;
        this.length = length;
        this.isPmem = isPmem;
        this.closed = false;
    }

    public static PmemMappedFile open(String path) throws IOException {
        Objects.requireNonNull(path);

        return pmem_map_file_open(path);
    }

    public static PmemMappedFile create(String path, long length, Set<PosixFilePermission> mode) throws IOException {
        Objects.requireNonNull(path);
        Objects.requireNonNull(mode);
        requirePositive(length);

        return pmem_map_file_create_excl(path, length, modeAsInt(mode));
    }

    public static PmemMappedFile openOrCreate(String path, long length, Set<PosixFilePermission> mode) throws IOException {
        Objects.requireNonNull(path);
        Objects.requireNonNull(mode);
        requirePositive(length);

        return pmem_map_file_create(path, length, modeAsInt(mode));
    }

    @Override
    public void close() throws IOException {
        if (closed) {
            return;
        }

        pmem_unmap(address, length);

        closed = true;
    }

    @Override
    protected void finalize() throws Throwable {
        try {
            close();
        } finally {
            super.finalize();
        }
    }

    public String path() {
        return path;
    }

    public long length() {
        return length;
    }

    public boolean isPmem() {
        return isPmem;
    }

    public boolean isHugeAligned() {
        return ((address & ~HUGE2M_MASK) == 0);
    }

    @Override
    public String toString() {
        return (new StringBuilder())
                .append("path ").append(path())
                .append(" address 0x").append(Long.toHexString(address))
                .append(" length ").append(length())
                .append(" isPmem ").append(isPmem())
                .append(" isHugeAligned ").append(isHugeAligned())
                .toString();
    }

    public PmemMappedFile putNoDrain(long offset, byte[] src, int index, int length) {
        Objects.requireNonNull(src);
        checkTheirBounds(src.length, index, length);
        checkBounds(offset, length);

        pmem_memcpy_nodrain(this.address + offset, src, index, length);
        return this;
    }

    public PmemMappedFile putNoFlush(long offset, byte[] src, int index, int length) {
        Objects.requireNonNull(src);
        checkTheirBounds(src.length, index, length);
        checkBounds(offset, length);

        pmem_memcpy_noflush(this.address + offset, src, index, length);
        return this;
    }

    public PmemMappedFile flush(long offset, long length) {
        checkBounds(offset, length);

        pmem_flush(this.address + offset, length);
        return this;
    }

    public PmemMappedFile drain() {
        pmem_drain();
        return this;
    }

    public PmemMappedFile msync(long offset, long length) throws IOException {
        checkBounds(offset, length);
        requireOpen();

        pmem_msync(this.address + offset, length);
        return this;
    }

    public ByteBuffer sliceAsReadOnlyBuffer(long offset, int length) {
        checkBounds(offset, length);

        return newDirectByteBufferView(this.address + offset, length).asReadOnlyBuffer();
    }

    public PmemOutputStream uniqueOutputStream(long offset, PmemExtendStrategy strategy, PmemPutStrategy putter) {
        return new PmemOutputStream(this, offset, strategy, putter);
    }

    private PmemMappedFile extend(long newLength) throws IOException {
        close();
        /*
         * Note that another thread could delete the file we just closed
         * between close and openOrCreate. In such a case, we would lose
         * the access to the file and the data it has. This is because we keep
         * no file descriptor and rely on pmem_map_file for file operations
         * such as open, mmap, and close.
         *
         * Also note that if another instance extends the same file we just
         * closed between close and openOrCreate to the size larger than
         * newLength, we will shrink the file by orCreate, leading to their
         * unexpected error. This is because pmem_map_file calls ftruncate to
         * set the length of the file.
         *
         * TODO keep file descriptor and call mmap, posix_fallocate, and munmap
         *      instead of using pmem_map_file.
         */

        return openOrCreate(path, newLength, Files.getPosixFilePermissions(Paths.get(path)));
    }

    private void requireOpen() throws IOException {
        if (closed) {
            throw new IOException();
        }
    }

    private void checkBounds(long offset) {
        checkBounds(offset, 0L);
    }

    private void checkBounds(long offset, long length) {
        checkTheirBounds(this.length, offset, length);
    }

    private static void checkTheirBounds(long theirLength, long offset, long length) {
        if (offset < 0L || length < 0L || theirLength < offset + length) {
            throw new IndexOutOfBoundsException((new StringBuilder())
                    .append("theirLength ").append(theirLength)
                    .append(" offset ").append(offset)
                    .append(" length ").append(length)
                    .toString());
        }
    }

    private static void requirePositive(long n) {
        if (n <= 0L) {
            throw new IllegalArgumentException(Long.toString(n));
        }
    }

    private static int modeAsInt(Set<PosixFilePermission> mode) {
        int i = 0;
        for (final PosixFilePermission permission : mode) {
            switch (permission) {
            case OTHERS_EXECUTE:
                i |= 0x001; // 0001
                break;
            case OTHERS_WRITE:
                i |= 0x002; // 0002
                break;
            case OTHERS_READ:
                i |= 0x004; // 0004
                break;
            case GROUP_EXECUTE:
                i |= 0x008; // 0010
                break;
            case GROUP_WRITE:
                i |= 0x010; // 0020
                break;
            case GROUP_READ:
                i |= 0x020; // 0040
                break;
            case OWNER_EXECUTE:
                i |= 0x040; // 0100
                break;
            case OWNER_WRITE:
                i |= 0x080; // 0200
                break;
            case OWNER_READ:
                i |= 0x100; // 0400
                break;
            default:
                throw new AssertionError(); // never be here
            }
        }
        return i;
    }

    public static final class PmemOutputStream extends OutputStream {
        private final PmemExtendStrategy extendStrategy;
        private final PmemPutStrategy putStrategy;

        private PmemMappedFile pmem; // non-final
        private long syncedUpTo;
        private long flushedUpTo;
        private long position;
        private boolean closed;

        public PmemOutputStream(PmemMappedFile pmem, long offset, PmemExtendStrategy extendStrategy, PmemPutStrategy putStrategy) {
            Objects.requireNonNull(pmem);
            Objects.requireNonNull(extendStrategy);
            Objects.requireNonNull(putStrategy);
            pmem.checkBounds(offset);

            this.pmem = pmem;
            this.extendStrategy = extendStrategy;
            this.putStrategy = putStrategy;
            this.syncedUpTo = this.flushedUpTo = this.position = offset;
            this.closed = false;
        }

        @Override
        public void close() throws IOException {
            // fast return
            if (this.closed) {
                return;
            }

            // another fast return
            if (pmem.closed) {
                // cannot do anything useful; just mark this stream as closed
                this.closed = true;
                return;
            }

            // flush and sync before close
            this.flush();
            sync();

            // close the underlying PMEM first
            pmem.close();

            // shrink to fit PMEM
            if (this.position < pmem.length()) {
                try (final RandomAccessFile file = new RandomAccessFile(pmem.path(), "rw")) {
                    file.setLength(this.position);
                }
            }

            // finally mark this stream as closed
            this.closed = true;
        }

        @Override
        protected void finalize() throws Throwable {
            try {
                close();
            } finally {
                super.finalize();
            }
        }

        public PmemMappedFile underlyingPmem() {
            return pmem;
        }

        @Override
        public final void write(int b) throws IOException {
            final byte[] src = new byte[1];
            src[0] = (byte) b;
            write(src);
        }

        @Override
        public final void write(byte[] src) throws IOException {
            write(src, 0, src.length);
        }

        @Override
        public final void write(byte[] src, int index, int length) throws IOException {
            Objects.requireNonNull(src);
            checkTheirBounds(src.length, index, length);
            this.requireOpen();
            pmem.requireOpen();

            // fast return
            if (length == 0) {
                return;
            }

            // extend if needed
            if (pmem.length() < this.position + length) {
                // flush and sync before extend
                this.flush();
                sync();

                pmem = pmem.extend(extendStrategy.newLength(pmem.length(), this.position, length));
            }

            putStrategy.put(pmem, this.position, src, index, length);
            this.position += length;

            /* Check invariant */
            assert (0L <= this.syncedUpTo);
            assert (this.syncedUpTo <= this.flushedUpTo);
            assert (this.flushedUpTo <= this.position);
            assert (this.position <= pmem.length());
        }

        @Override
        public void flush() throws IOException {
            if (pmem.isPmem()) {
                putStrategy.flush(pmem, flushedUpTo, position - flushedUpTo);
            }
            // else do nothing

            flushedUpTo = position;
        }

        public void sync() throws IOException {
            if (pmem.isPmem()) {
                pmem.drain();
            } else {
                requireOpen();
                pmem.msync(syncedUpTo, flushedUpTo - syncedUpTo);
            }

            syncedUpTo = flushedUpTo;
        }

        private void requireOpen() throws IOException {
            if (closed) {
                throw new IOException();
            }
        }
    }

    public enum PmemExtendStrategy {
        EXTEND_TO_FIT {
            @Override
            long newLength(long pmemLength, long pmemPosition, int writeLength) {
                return pmemPosition + writeLength;
            }
        },

        EXTEND_DOUBLY {
            @Override
            long newLength(long pmemLength, long pmemPosition, int writeLength) {
                final long tail = pmemPosition + writeLength;
                long newLengthCandidate = pmemLength;
                while (newLengthCandidate < tail) {
                    newLengthCandidate *= 2;
                }
                return newLengthCandidate;
            }
        };

        abstract long newLength(long pmemLength, long pmemPosition, int writeLength);
    }

    public enum PmemPutStrategy {
        PUT_NO_DRAIN {
            @Override
            void put(PmemMappedFile pmem, long offset, byte[] src, int index, int length) {
                pmem.putNoDrain(offset, src, index, length);
            }

            @Override
            void flush(PmemMappedFile pmem, long offset, long length) {
                /* Do nothing */
            }
        },

        PUT_NO_FLUSH {
            @Override
            void put(PmemMappedFile pmem, long offset, byte[] src, int index, int length) {
                pmem.putNoFlush(offset, src, index, length);
            }

            @Override
            void flush(PmemMappedFile pmem, long offset, long length) {
                pmem.flush(offset, length);
            }
        };

        abstract void put(PmemMappedFile pmem, long offset, byte[] src, int index, int length);

        abstract void flush(PmemMappedFile pmem, long offset, long length);
    }
}
