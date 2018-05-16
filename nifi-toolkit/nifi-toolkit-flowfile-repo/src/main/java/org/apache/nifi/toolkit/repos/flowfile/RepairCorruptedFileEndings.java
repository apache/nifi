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

package org.apache.nifi.toolkit.repos.flowfile;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.RandomAccessFile;
import java.nio.file.Files;
import java.nio.file.StandardCopyOption;
import java.util.List;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.nifi.stream.io.LimitingInputStream;
import org.apache.nifi.stream.io.StreamUtils;

public class RepairCorruptedFileEndings {
    private static final Pattern PARTITION_FILE_PATTERN = Pattern.compile("partition\\-\\d+");

    private static void printUsage() {
        System.out.println("Whenever a sudden power loss occurs, it is common with some operating systems for files that are being written to ");
        System.out.println("to contain many NUL characters (hex 0) at the end of the file upon restart. If this happens to the FlowFile repository, ");
        System.out.println("NiFi will be unable to recover, because it cannot properly read the repository. This utility attempts to read the FlowFile ");
        System.out.println("Repository and write out a new copy of the repository, where the new copy does not contain the trailing NUL characters so ");
        System.out.println("NiFi can be restarted by pointing at the new FlowFile Repository.");
        System.out.println("Typically, this problem can be identified by seeing an error in the NiFi logs at startup, indicating either:");
        System.out.println();
        System.out.println("Caused by: java.io.IOException: Expected to read a Sentinel Byte of '1' but got a value of '0' instead");
        System.out.println();
        System.out.println("or:");
        System.out.println();
        System.out.println("Caused by: java.lang.IllegalArgumentException: No enum constant org.wali.UpdateType.");
        System.out.println();
        System.out.println();
        System.out.println("Usage:");
        System.out.println("java " + RepairCorruptedFileEndings.class.getCanonicalName() + " <repo input directory> <repo destination directory>");
        System.out.println();
        System.out.println("<repo input directory>: The existing FlowFile Repository Directory that contains corrupt data");
        System.out.println("<repo destination directory>: The directory to write the repaired repository to");
        System.out.println();
    }

    public static void main(final String[] args) {
        if (args.length != 2) {
            printUsage();
            return;
        }

        final File inputDir = new File(args[0]);
        if (!inputDir.exists()) {
            System.out.println("Input Repository Directory " + inputDir + " does not exist");
            return;
        }

        final File[] inputFiles = inputDir.listFiles();
        if (inputFiles == null) {
            System.out.println("Could not access files within input Repository Directory " + inputDir);
            return;
        }

        final List<File> partitionDirs = Stream.of(inputFiles)
            .filter(RepairCorruptedFileEndings::isPartitionDirectory)
            .collect(Collectors.toList());

        if (partitionDirs.isEmpty()) {
            System.out.println("Found no partitions within input Repository Directory " + inputDir);
            return;
        }

        final File outputDir = new File(args[1]);
        if (outputDir.exists()) {
            final File[] children = outputDir.listFiles();
            if (children == null) {
                System.out.println("Cannot access output Repository Directory " + outputDir);
                return;
            }

            if (children.length > 0) {
                System.out.println("Output Repository Directory " + outputDir + " already exists and has files or sub-directories. "
                    + "The output directory must either not exist or be empty.");
                return;
            }
        } else if (!outputDir.mkdirs()) {
            System.out.println("Failed to create output Repository Directory " + outputDir);
            return;
        }

        final List<File> nonPartitionDirFiles = Stream.of(inputFiles)
            .filter(f -> !isPartitionDirectory(f))
            .filter(f -> !f.getName().equals("wali.lock"))
            .collect(Collectors.toList());

        for (final File nonPartitionFile : nonPartitionDirFiles) {
            final File destination = new File(outputDir, nonPartitionFile.getName());
            try {
                copy(nonPartitionFile, destination);
            } catch (final IOException e) {
                System.out.println("Failed to copy source file " + nonPartitionFile + " to destination file " + destination);
                e.printStackTrace();
            }
        }

        int fullCopies = 0;
        int partialCopies = 0;

        for (final File partitionDir : partitionDirs) {
            final File[] partitionFiles = partitionDir.listFiles();
            if (partitionFiles == null) {
                System.out.println("Could not access children of input sub-directory " + partitionDir);
                return;
            }

            final File outputPartitionDir = new File(outputDir, partitionDir.getName());
            if (!outputPartitionDir.mkdirs()) {
                System.out.println("Failed to created output directory " + outputPartitionDir);
                return;
            }

            for (final File partitionFile : partitionFiles) {
                final File destinationFile = new File(outputPartitionDir, partitionFile.getName());

                // All journal files follow the pattern of:
                // <journal entry> <TRANSACTION_CONTINUE | TRANSACTION_COMMIT> <journal entry> <TRANSACTION_CONTINUE | TRANSACTION_COMMIT> ...
                // The TRANSACTION_CONTINUE byte is a 1 while the TRANSACTION_COMMIT byte is a 2. So if we have 0's at the end then we know
                // that we can simply truncate up until the point where we encounter the first of the of the trailing zeroes. At that point,
                // we know that we are done. It is possible that the repo will still be 'corrupt' in that only part of a transaction was
                // written out. However, this is okay because the repo will recover from this on restart. What it does NOT properly recover
                // from on restart is when the file ends with a bunch of 0's because it believes that the Transaction ID is zero and then
                // it reads in 0 bytes for the "Update Type" and as a result we get an invalid enum name because it thinks that the name of
                // the UpdateType is an empty string because it's a string of length 0.
                final int trailingZeroes;
                try {
                    trailingZeroes = countTrailingZeroes(partitionFile);
                } catch (final Exception e) {
                    System.out.println("Failed to read input file " + partitionFile);
                    e.printStackTrace();
                    return;
                }

                if (trailingZeroes > 0) {
                    final long goodLength = partitionFile.length() - trailingZeroes;

                    try {
                        copy(partitionFile, destinationFile, goodLength);
                        partialCopies++;
                    } catch (final Exception e) {
                        System.out.println("Failed to copy " + goodLength + " bytes from " + partitionFile + " to " + destinationFile);
                        e.printStackTrace();
                        return;
                    }
                } else {
                    try {
                        copy(partitionFile, destinationFile);
                    } catch (final Exception e) {
                        System.out.println("Failed to copy entire file from " + partitionFile + " to " + destinationFile);
                        e.printStackTrace();
                        return;
                    }

                    fullCopies++;
                }
            }
        }

        System.out.println("Successfully copied " + fullCopies + " journal files fully and truncated " + partialCopies + " journal files in output directory");
    }

    private static boolean isPartitionDirectory(final File file) {
        return PARTITION_FILE_PATTERN.matcher(file.getName()).matches();
    }

    private static void copy(final File input, final File destination) throws IOException {
        if (input.isFile()) {
            copyFile(input, destination);
            return;
        } else {
            copyDirectory(input, destination);
        }
    }

    private static void copyDirectory(final File input, final File destination) throws IOException {
        if (!destination.exists() && !destination.mkdirs()) {
            System.out.println("Failed to copy input directory " + input + " to destination because destination directory " + destination
                + " does not exist and could not be created");
            return;
        }

        final File[] children = input.listFiles();
        if (children == null) {
            System.out.println("Failed to copy input directory " + input + " to destination because could not access files of input directory");
            return;
        }

        for (final File child : children) {
            final File destinationChild = new File(destination, child.getName());
            copy(child, destinationChild);
        }
    }

    private static void copyFile(final File input, final File destination) throws IOException {
        if (!input.exists()) {
            return;
        }

        Files.copy(input.toPath(), destination.toPath(), StandardCopyOption.COPY_ATTRIBUTES);
    }

    private static void copy(final File input, final File destination, final long length) throws IOException {
        try (final InputStream fis = new FileInputStream(input);
            final LimitingInputStream in = new LimitingInputStream(fis, length);
            final OutputStream fos = new FileOutputStream(destination)) {
            StreamUtils.copy(in, fos);
        }
    }

    static int countTrailingZeroes(final File partitionFile) throws IOException {
        final RandomAccessFile raf = new RandomAccessFile(partitionFile, "r");

        long startPos = partitionFile.length() - 4096;

        int count = 0;
        boolean reachedStartOfFile = false;
        while (!reachedStartOfFile) {
            int bufferLength = 4096;

            if (startPos < 0) {
                bufferLength = (int) (startPos + 4096);
                startPos = 0;
                reachedStartOfFile = true;
            }

            raf.seek(startPos);

            final byte[] buffer = new byte[bufferLength];
            final int read = fillBuffer(raf, buffer);

            for (int i = read - 1; i >= 0; i--) {
                final byte b = buffer[i];
                if (b == 0) {
                    count++;
                } else {
                    return count;
                }
            }

            startPos -= 4096;
        }

        return count;
    }


    private static int fillBuffer(final RandomAccessFile source, final byte[] destination) throws IOException {
        int bytesRead = 0;
        int len;
        while (bytesRead < destination.length) {
            len = source.read(destination, bytesRead, destination.length - bytesRead);
            if (len < 0) {
                break;
            }

            bytesRead += len;
        }

        return bytesRead;
    }
}
