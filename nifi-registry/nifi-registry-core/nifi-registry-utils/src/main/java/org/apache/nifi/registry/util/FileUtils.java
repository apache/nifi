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
package org.apache.nifi.registry.util;

import java.io.Closeable;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FilenameFilter;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Collection;

import org.slf4j.Logger;

/**
 * A utility class containing a few useful static methods to do typical IO
 * operations.
 *
 */
public class FileUtils {

    public static final long TRANSFER_CHUNK_SIZE_BYTES = 1024 * 1024 * 8; //8 MB chunks
    public static final long MILLIS_BETWEEN_ATTEMPTS = 50L;

    /**
     * Closes the given closeable quietly - no logging, no exceptions...
     *
     * @param closeable the thing to close
     */
    public static void closeQuietly(final Closeable closeable) {
        if (null != closeable) {
            try {
                closeable.close();
            } catch (final IOException io) {/*IGNORE*/

            }
        }
    }

    /**
     * Releases the given lock quietly no logging, no exception
     *
     * @param lock the lock to release
     */
    public static void releaseQuietly(final FileLock lock) {
        if (null != lock) {
            try {
                lock.release();
            } catch (final IOException io) {
                /*IGNORE*/
            }
        }
    }

    /* Superseded by renamed class bellow */
    @Deprecated
    public static void ensureDirectoryExistAndCanAccess(final File dir) throws IOException {
        ensureDirectoryExistAndCanReadAndWrite(dir);
    }

    public static void ensureDirectoryExistAndCanReadAndWrite(final File dir) throws IOException {
        if (dir.exists() && !dir.isDirectory()) {
            throw new IOException(dir.getAbsolutePath() + " is not a directory");
        } else if (!dir.exists()) {
            final boolean made = dir.mkdirs();
            if (!made) {
                throw new IOException(dir.getAbsolutePath() + " could not be created");
            }
        }
        if (!(dir.canRead() && dir.canWrite())) {
            throw new IOException(dir.getAbsolutePath() + " directory does not have read/write privilege");
        }
    }

    public static void ensureDirectoryExistAndCanRead(final File dir) throws IOException {
        if (dir.exists() && !dir.isDirectory()) {
            throw new IOException(dir.getAbsolutePath() + " is not a directory");
        } else if (!dir.exists()) {
            final boolean made = dir.mkdirs();
            if (!made) {
                throw new IOException(dir.getAbsolutePath() + " could not be created");
            }
        }
        if (!dir.canRead()) {
            throw new IOException(dir.getAbsolutePath() + " directory does not have read privilege");
        }
    }

    /**
     * Copies the given source file to the given destination file. The given destination will be overwritten if it already exists.
     *
     * @param source the file to copy
     * @param destination the file to copy to
     * @param lockInputFile if true will lock input file during copy; if false will not
     * @param lockOutputFile if true will lock output file during copy; if false will not
     * @param move if true will perform what is effectively a move operation rather than a pure copy. This allows for potentially highly efficient movement of the file but if not possible this will
     * revert to a copy then delete behavior. If false, then the file is copied and the source file is retained. If a true rename/move occurs then no lock is held during that time.
     * @param logger if failures occur, they will be logged to this logger if possible. If this logger is null, an IOException will instead be thrown, indicating the problem.
     * @return long number of bytes copied
     * @throws FileNotFoundException if the source file could not be found
     * @throws IOException if unable to read or write the underlying streams
     * @throws SecurityException if a security manager denies the needed file operations
     */
    public static long copyFile(final File source, final File destination, final boolean lockInputFile, final boolean lockOutputFile, final boolean move, final Logger logger)
            throws FileNotFoundException, IOException {

        FileInputStream fis = null;
        FileOutputStream fos = null;
        FileLock inLock = null;
        FileLock outLock = null;
        long fileSize = 0L;
        if (!source.canRead()) {
            throw new IOException("Must at least have read permission");

        }
        if (move && source.renameTo(destination)) {
            fileSize = destination.length();
        } else {
            try {
                fis = new FileInputStream(source);
                fos = new FileOutputStream(destination);
                final FileChannel in = fis.getChannel();
                final FileChannel out = fos.getChannel();
                if (lockInputFile) {
                    inLock = in.tryLock(0, Long.MAX_VALUE, true);
                    if (null == inLock) {
                        throw new IOException("Unable to obtain shared file lock for: " + source.getAbsolutePath());
                    }
                }
                if (lockOutputFile) {
                    outLock = out.tryLock(0, Long.MAX_VALUE, false);
                    if (null == outLock) {
                        throw new IOException("Unable to obtain exclusive file lock for: " + destination.getAbsolutePath());
                    }
                }
                long bytesWritten = 0;
                do {
                    bytesWritten += out.transferFrom(in, bytesWritten, TRANSFER_CHUNK_SIZE_BYTES);
                    fileSize = in.size();
                } while (bytesWritten < fileSize);
                out.force(false);
                FileUtils.closeQuietly(fos);
                FileUtils.closeQuietly(fis);
                fos = null;
                fis = null;
                if (move && !FileUtils.deleteFile(source, null, 5)) {
                    if (logger == null) {
                        FileUtils.deleteFile(destination, null, 5);
                        throw new IOException("Could not remove file " + source.getAbsolutePath());
                    } else {
                        logger.warn("Configured to delete source file when renaming/move not successful.  However, unable to delete file at: " + source.getAbsolutePath());
                    }
                }
            } finally {
                FileUtils.releaseQuietly(inLock);
                FileUtils.releaseQuietly(outLock);
                FileUtils.closeQuietly(fos);
                FileUtils.closeQuietly(fis);
            }
        }
        return fileSize;
    }

    /**
     * Copies the given source file to the given destination file. The given destination will be overwritten if it already exists.
     *
     * @param source the file to copy from
     * @param destination the file to copy to
     * @param lockInputFile if true will lock input file during copy; if false will not
     * @param lockOutputFile if true will lock output file during copy; if false will not
     * @param logger the logger to use
     * @return long number of bytes copied
     * @throws FileNotFoundException if the source file could not be found
     * @throws IOException if unable to read or write to file
     * @throws SecurityException if a security manager denies the needed file operations
     */
    public static long copyFile(final File source, final File destination, final boolean lockInputFile, final boolean lockOutputFile, final Logger logger) throws FileNotFoundException, IOException {
        return FileUtils.copyFile(source, destination, lockInputFile, lockOutputFile, false, logger);
    }

    public static long copyFile(final File source, final OutputStream stream, final boolean closeOutputStream, final boolean lockInputFile) throws FileNotFoundException, IOException {
        FileInputStream fis = null;
        FileLock inLock = null;
        long fileSize = 0L;
        try {
            fis = new FileInputStream(source);
            final FileChannel in = fis.getChannel();
            if (lockInputFile) {
                inLock = in.tryLock(0, Long.MAX_VALUE, true);
                if (inLock == null) {
                    throw new IOException("Unable to obtain exclusive file lock for: " + source.getAbsolutePath());
                }

            }

            byte[] buffer = new byte[1 << 18]; //256 KB
            int bytesRead = -1;
            while ((bytesRead = fis.read(buffer)) != -1) {
                stream.write(buffer, 0, bytesRead);
            }
            in.force(false);
            stream.flush();
            fileSize = in.size();
        } finally {
            FileUtils.releaseQuietly(inLock);
            FileUtils.closeQuietly(fis);
            if (closeOutputStream) {
                FileUtils.closeQuietly(stream);
            }
        }
        return fileSize;
    }

    public static long copyFile(final InputStream stream, final File destination, final boolean closeInputStream, final boolean lockOutputFile) throws FileNotFoundException, IOException {
        final Path destPath = destination.toPath();
        final long size = Files.copy(stream, destPath);
        if (closeInputStream) {
            stream.close();
        }
        return size;
    }

    /**
     * Deletes the given file. If the given file exists but could not be deleted
     * this will be printed as a warning to the given logger
     *
     * @param file to delete
     * @param logger to notify
     * @return true if deleted
     */
    public static boolean deleteFile(final File file, final Logger logger) {
        return FileUtils.deleteFile(file, logger, 1);
    }

    /**
     * Deletes the given file. If the given file exists but could not be deleted
     * this will be printed as a warning to the given logger
     *
     * @param file to delete
     * @param logger to notify
     * @param attempts indicates how many times an attempt to delete should be
     * made
     * @return true if given file no longer exists
     */
    public static boolean deleteFile(final File file, final Logger logger, final int attempts) {
        if (file == null) {
            return false;
        }
        boolean isGone = false;
        try {
            if (file.exists()) {
                final int effectiveAttempts = Math.max(1, attempts);
                for (int i = 0; i < effectiveAttempts && !isGone; i++) {
                    isGone = file.delete() || !file.exists();
                    if (!isGone && (effectiveAttempts - i) > 1) {
                        FileUtils.sleepQuietly(MILLIS_BETWEEN_ATTEMPTS);
                    }
                }
                if (!isGone && logger != null) {
                    logger.warn("File appears to exist but unable to delete file: " + file.getAbsolutePath());
                }
            }
        } catch (final Throwable t) {
            if (logger != null) {
                logger.warn("Unable to delete file: '" + file.getAbsolutePath() + "' due to " + t);
            }
        }
        return isGone;
    }

    /**
     * Deletes all files (not directories..) in the given directory (non
     * recursive) that match the given filename filter. If any file cannot be
     * deleted then this is printed at warn to the given logger.
     *
     * @param directory to delete contents of
     * @param filter if null then no filter is used
     * @param logger to notify
     * @throws IOException if abstract pathname does not denote a directory, or
     * if an I/O error occurs
     */
    public static void deleteFilesInDirectory(final File directory, final FilenameFilter filter, final Logger logger) throws IOException {
        FileUtils.deleteFilesInDirectory(directory, filter, logger, false);
    }

    /**
     * Deletes all files (not directories) in the given directory (recursive)
     * that match the given filename filter. If any file cannot be deleted then
     * this is printed at warn to the given logger.
     *
     * @param directory to delete contents of
     * @param filter if null then no filter is used
     * @param logger to notify
     * @param recurse true if should recurse
     * @throws IOException if abstract pathname does not denote a directory, or
     * if an I/O error occurs
     */
    public static void deleteFilesInDirectory(final File directory, final FilenameFilter filter, final Logger logger, final boolean recurse) throws IOException {
        FileUtils.deleteFilesInDirectory(directory, filter, logger, recurse, false);
    }

    /**
     * Deletes all files (not directories) in the given directory (recursive)
     * that match the given filename filter. If any file cannot be deleted then
     * this is printed at warn to the given logger.
     *
     * @param directory to delete contents of
     * @param filter if null then no filter is used
     * @param logger to notify
     * @param recurse will look for contents of sub directories.
     * @param deleteEmptyDirectories default is false; if true will delete
     * directories found that are empty
     * @throws IOException if abstract pathname does not denote a directory, or
     * if an I/O error occurs
     */
    public static void deleteFilesInDirectory(final File directory, final FilenameFilter filter, final Logger logger, final boolean recurse, final boolean deleteEmptyDirectories) throws IOException {
        // ensure the specified directory is actually a directory and that it exists
        if (null != directory && directory.isDirectory()) {
            final File ingestFiles[] = directory.listFiles();
            if (ingestFiles == null) {
                // null if abstract pathname does not denote a directory, or if an I/O error occurs
                throw new IOException("Unable to list directory content in: " + directory.getAbsolutePath());
            }
            for (File ingestFile : ingestFiles) {
                boolean process = (filter == null) ? true : filter.accept(directory, ingestFile.getName());
                if (ingestFile.isFile() && process) {
                    FileUtils.deleteFile(ingestFile, logger, 3);
                }
                if (ingestFile.isDirectory() && recurse) {
                    FileUtils.deleteFilesInDirectory(ingestFile, filter, logger, recurse, deleteEmptyDirectories);
                    if (deleteEmptyDirectories && ingestFile.list().length == 0) {
                        FileUtils.deleteFile(ingestFile, logger, 3);
                    }
                }
            }
        }
    }

    /**
     * Deletes given files.
     *
     * @param files to delete
     * @param recurse will recurse
     * @throws IOException if issues deleting files
     */
    public static void deleteFiles(final Collection<File> files, final boolean recurse) throws IOException {
        for (final File file : files) {
            FileUtils.deleteFile(file, recurse);
        }
    }

    public static void deleteFile(final File file, final boolean recurse) throws IOException {
        final File[] list = file.listFiles();
        if (file.isDirectory() && recurse && list != null) {
            FileUtils.deleteFiles(Arrays.asList(list), recurse);
        }
        //now delete the file itself regardless of whether it is plain file or a directory
        if (!FileUtils.deleteFile(file, null, 5)) {
            throw new IOException("Unable to delete " + file.getAbsolutePath());
        }
    }

    public static void sleepQuietly(final long millis) {
        try {
            Thread.sleep(millis);
        } catch (final InterruptedException ex) {
            /* do nothing */
        }
    }


    // The invalid character list is derived from this Stackoverflow page.
    // https://stackoverflow.com/questions/1155107/is-there-a-cross-platform-java-method-to-remove-filename-special-chars
    private final static int[] INVALID_CHARS = {34, 60, 62, 124, 0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15,
            16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31, 58, 42, 63, 92, 47, 32};

    static {
        Arrays.sort(INVALID_CHARS);
    }

    /**
     * Replaces invalid characters for a file system name within a given filename string to underscore '_'.
     * Be careful not to pass a file path as this method replaces path delimiter characters (i.e forward/back slashes).
     * @param filename The filename to clean
     * @return sanitized filename
     */
    public static String sanitizeFilename(String filename) {
        if (filename == null || filename.isEmpty()) {
            return filename;
        }
        int codePointCount = filename.codePointCount(0, filename.length());

        final StringBuilder cleanName = new StringBuilder();
        for (int i = 0; i < codePointCount; i++) {
            int c = filename.codePointAt(i);
            if (Arrays.binarySearch(INVALID_CHARS, c) < 0) {
                cleanName.appendCodePoint(c);
            } else {
                cleanName.append('_');
            }
        }
        return cleanName.toString();
    }
}
