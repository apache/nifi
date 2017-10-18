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
package org.apache.nifi.util.file;

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
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Random;

import org.slf4j.Logger;

/**
 * A utility class containing a few useful static methods to do typical IO operations.
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

    public static void ensureDirectoryExistAndCanAccess(final File dir) throws IOException {
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

    /**
     * Deletes the given file. If the given file exists but could not be deleted this will be printed as a warning to the given logger
     *
     * @param file the file to delete
     * @param logger the logger to provide logging information to about the operation
     * @return true if given file no longer exists
     */
    public static boolean deleteFile(final File file, final Logger logger) {
        return FileUtils.deleteFile(file, logger, 1);
    }

    /**
     * Deletes the given file. If the given file exists but could not be deleted this will be printed as a warning to the given logger
     *
     * @param file the file to delete
     * @param logger the logger to write to
     * @param attempts indicates how many times an attempt to delete should be made
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
     * Deletes all of the given files. If any exist and cannot be deleted that will be printed at warn to the given logger.
     *
     * @param files can be null
     * @param logger can be null
     */
    public static void deleteFile(final List<File> files, final Logger logger) {
        FileUtils.deleteFile(files, logger, 1);
    }

    /**
     * Deletes all of the given files. If any exist and cannot be deleted that will be printed at warn to the given logger.
     *
     * @param files can be null
     * @param logger can be null
     * @param attempts indicates how many times an attempt should be made to delete each file
     */
    public static void deleteFile(final List<File> files, final Logger logger, final int attempts) {
        if (null == files || files.isEmpty()) {
            return;
        }
        final int effectiveAttempts = Math.max(1, attempts);
        for (final File file : files) {
            try {
                boolean isGone = false;
                for (int i = 0; i < effectiveAttempts && !isGone; i++) {
                    isGone = file.delete() || !file.exists();
                    if (!isGone && (effectiveAttempts - i) > 1) {
                        FileUtils.sleepQuietly(MILLIS_BETWEEN_ATTEMPTS);
                    }
                }
                if (!isGone && logger != null) {
                    logger.warn("File appears to exist but unable to delete file: " + file.getAbsolutePath());
                }
            } catch (final Throwable t) {
                if (null != logger) {
                    logger.warn("Unable to delete file given from path: '" + file.getPath() + "' due to " + t);
                }
            }
        }
    }

    /**
     * Deletes all files (not directories..) in the given directory (non recursive) that match the given filename filter. If any file cannot be deleted then this is printed at warn to the given
     * logger.
     *
     * @param directory the directory to scan for files to delete
     * @param filter if null then no filter is used
     * @param logger the logger to use
     */
    public static void deleteFilesInDir(final File directory, final FilenameFilter filter, final Logger logger) {
        FileUtils.deleteFilesInDir(directory, filter, logger, false);
    }

    /**
     * Deletes all files (not directories) in the given directory (recursive) that match the given filename filter. If any file cannot be deleted then this is printed at warn to the given logger.
     *
     * @param directory the directory to scan
     * @param filter if null then no filter is used
     * @param logger the logger to use
     * @param recurse indicates whether to recurse subdirectories
     */
    public static void deleteFilesInDir(final File directory, final FilenameFilter filter, final Logger logger, final boolean recurse) {
        FileUtils.deleteFilesInDir(directory, filter, logger, recurse, false);
    }

    /**
     * Deletes all files (not directories) in the given directory (recursive) that match the given filename filter. If any file cannot be deleted then this is printed at warn to the given logger.
     *
     * @param directory the directory to scan
     * @param filter if null then no filter is used
     * @param logger the logger
     * @param recurse whether to recurse subdirectories or not
     * @param deleteEmptyDirectories default is false; if true will delete directories found that are empty
     */
    public static void deleteFilesInDir(final File directory, final FilenameFilter filter, final Logger logger, final boolean recurse, final boolean deleteEmptyDirectories) {
        // ensure the specified directory is actually a directory and that it exists
        if (null != directory && directory.isDirectory()) {
            final File ingestFiles[] = directory.listFiles();
            for (File ingestFile : ingestFiles) {
                boolean process = (filter == null) ? true : filter.accept(directory, ingestFile.getName());
                if (ingestFile.isFile() && process) {
                    FileUtils.deleteFile(ingestFile, logger, 3);
                }
                if (ingestFile.isDirectory() && recurse) {
                    FileUtils.deleteFilesInDir(ingestFile, filter, logger, recurse, deleteEmptyDirectories);
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
     * @param files the files to delete
     * @param recurse will recurse if true; false otherwise
     * @throws IOException if any issues deleting specified files
     */
    public static void deleteFiles(final Collection<File> files, final boolean recurse) throws IOException {
        for (final File file : files) {
            FileUtils.deleteFile(file, recurse);
        }
    }

    public static void deleteFile(final File file, final boolean recurse) throws IOException {
        if (file.isDirectory() && recurse) {
            FileUtils.deleteFiles(Arrays.asList(file.listFiles()), recurse);
        }
        //now delete the file itself regardless of whether it is plain file or a directory
        if (!FileUtils.deleteFile(file, null, 5)) {
            throw new IOException("Unable to delete " + file.getAbsolutePath());
        }
    }

    /**
     * Randomly generates a sequence of bytes and overwrites the contents of the file a number of times. The file is then deleted.
     *
     * @param file File to be overwritten a number of times and, ultimately, deleted
     * @param passes Number of times file should be overwritten
     * @throws IOException if something makes shredding or deleting a problem
     */
    public static void shredFile(final File file, final int passes)
            throws IOException {
        final Random generator = new Random();
        final long fileLength = file.length();
        final int byteArraySize = (int) Math.min(fileLength, 1048576); // 1MB
        final byte[] b = new byte[byteArraySize];
        final long numOfRandomWrites = (fileLength / b.length) + 1;
        final FileOutputStream fos = new FileOutputStream(file);
        try {
            // Over write file contents (passes) times
            final FileChannel channel = fos.getChannel();
            for (int i = 0; i < passes; i++) {
                generator.nextBytes(b);
                for (int j = 0; j <= numOfRandomWrites; j++) {
                    fos.write(b);
                }
                fos.flush();
                channel.position(0);
            }
            // Write out "0" for each byte in the file
            Arrays.fill(b, (byte) 0);
            for (int j = 0; j < numOfRandomWrites; j++) {
                fos.write(b);
            }
            fos.flush();
            fos.close();
            // Try to delete the file a few times
            if (!FileUtils.deleteFile(file, null, 5)) {
                throw new IOException("Failed to delete file after shredding");
            }

        } finally {
            FileUtils.closeQuietly(fos);
        }
    }

    public static long copy(final InputStream in, final OutputStream out) throws IOException {
        final byte[] buffer = new byte[65536];
        long copied = 0L;
        int len;
        while ((len = in.read(buffer)) > 0) {
            out.write(buffer, 0, len);
            copied += len;
        }

        return copied;
    }

    public static long copyBytes(final byte[] bytes, final File destination, final boolean lockOutputFile) throws FileNotFoundException, IOException {
        FileOutputStream fos = null;
        FileLock outLock = null;
        long fileSize = 0L;
        try {
            fos = new FileOutputStream(destination);
            final FileChannel out = fos.getChannel();
            if (lockOutputFile) {
                outLock = out.tryLock(0, Long.MAX_VALUE, false);
                if (null == outLock) {
                    throw new IOException("Unable to obtain exclusive file lock for: " + destination.getAbsolutePath());
                }
            }
            fos.write(bytes);
            fos.flush();
            fileSize = bytes.length;
        } finally {
            FileUtils.releaseQuietly(outLock);
            FileUtils.closeQuietly(fos);
        }
        return fileSize;
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
     * Renames the given file from the source path to the destination path. This handles multiple attempts. This should only be used to rename within a given directory. Renaming across directories
     * might not work well. See the <code>File.renameTo</code> for more information.
     *
     * @param source the file to rename
     * @param destination the file path to rename to
     * @param maxAttempts the max number of attempts to attempt the rename
     * @throws IOException if rename isn't successful
     */
    public static void renameFile(final File source, final File destination, final int maxAttempts) throws IOException {
        FileUtils.renameFile(source, destination, maxAttempts, false);
    }

    /**
     * Renames the given file from the source path to the destination path. This handles multiple attempts. This should only be used to rename within a given directory. Renaming across directories
     * might not work well. See the <code>File.renameTo</code> for more information.
     *
     * @param source the file to rename
     * @param destination the file path to rename to
     * @param maxAttempts the max number of attempts to attempt the rename
     * @param replace if true and a rename attempt fails will check if a file is already at the destination path. If so it will delete that file and attempt the rename according the remaining
     * maxAttempts. If false, any conflicting files will be left as they were and the rename attempts will fail if conflicting.
     * @throws IOException if rename isn't successful
     */
    public static void renameFile(final File source, final File destination, final int maxAttempts, final boolean replace) throws IOException {
        final int attempts = (replace || maxAttempts < 1) ? Math.max(2, maxAttempts) : maxAttempts;
        boolean renamed = false;
        for (int i = 0; i < attempts; i++) {
            renamed = source.renameTo(destination);
            if (!renamed) {
                FileUtils.deleteFile(destination, null, 5);
            } else {
                break; //rename has succeeded
            }
        }
        if (!renamed) {
            throw new IOException("Attempted " + maxAttempts + " times but unable to rename from \'" + source.getPath() + "\' to \'" + destination.getPath() + "\'");

        }
    }

    public static void sleepQuietly(final long millis) {
        try {
            Thread.sleep(millis);
        } catch (final InterruptedException ex) {
            /* do nothing */
        }
    }

    /**
     * Syncs a primary copy of a file with the copy in the restore directory. If the restore directory does not have a file and the primary has a file, the the primary's file is copied to the restore
     * directory. Else if the restore directory has a file, but the primary does not, then the restore's file is copied to the primary directory. Else if the primary file is different than the restore
     * file, then an IllegalStateException is thrown. Otherwise, if neither file exists, then no syncing is performed.
     *
     * @param primaryFile the primary file
     * @param restoreFile the restore file
     * @param logger a logger
     * @throws IOException if an I/O problem was encountered during syncing
     * @throws IllegalStateException if the primary and restore copies exist but are different
     */
    public static void syncWithRestore(final File primaryFile, final File restoreFile, final Logger logger)
            throws IOException {

        if (primaryFile.exists() && !restoreFile.exists()) {
            // copy primary file to restore
            copyFile(primaryFile, restoreFile, false, false, logger);
        } else if (restoreFile.exists() && !primaryFile.exists()) {
            // copy restore file to primary
            copyFile(restoreFile, primaryFile, false, false, logger);
        } else if (primaryFile.exists() && restoreFile.exists() && !isSame(primaryFile, restoreFile)) {
            throw new IllegalStateException(String.format("Primary file '%s' is different than restore file '%s'",
                    primaryFile.getAbsoluteFile(), restoreFile.getAbsolutePath()));
        }
    }

    /**
     * Returns true if the given files are the same according to their MD5 hash.
     *
     * @param file1 a file
     * @param file2 a file
     * @return true if the files are the same; false otherwise
     * @throws IOException if the MD5 hash could not be computed
     */
    public static boolean isSame(final File file1, final File file2) throws IOException {
        return Arrays.equals(computeMd5Digest(file1), computeMd5Digest(file2));
    }

    /**
     * Returns the MD5 hash of the given file.
     *
     * @param file a file
     * @return the MD5 hash
     * @throws IOException if the MD5 hash could not be computed
     */
    public static byte[] computeMd5Digest(final File file) throws IOException {
        try (final FileInputStream fis = new FileInputStream(file)) {
            return computeMd5Digest(fis);
        }
    }

    /**
     * Returns the MD5 hash of the given stream.
     *
     * @param stream an input stream
     * @return the MD5 hash
     * @throws IOException if the MD5 hash could not be computed
     */
    public static byte[] computeMd5Digest(final InputStream stream) throws IOException {
        final MessageDigest digest;
        try {
            digest = MessageDigest.getInstance("MD5");
        } catch (final NoSuchAlgorithmException nsae) {
            throw new IOException(nsae);
        }


        int len;
        final byte[] buffer = new byte[8192];
        while ((len = stream.read(buffer)) > -1) {
            if (len > 0) {
                digest.update(buffer, 0, len);
            }
        }

        return digest.digest();
    }

    /**
     * Returns the capacity for a given path
     * @param path path
     * @return total space
     */
    public static long getContainerCapacity(final Path path) {
        return path.toFile().getTotalSpace();
    }

    /**
     * Returns the free capacity for a given path
     * @param path path
     * @return usable space
     */
    public static long getContainerUsableSpace(final Path path) {
        return path.toFile().getUsableSpace();
    }

}
