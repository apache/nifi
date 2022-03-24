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
package org.apache.nifi.util;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.Map;
import java.util.UUID;

public class Unpackage {

    private static void printUsage() {
        System.out.println("java " + Unpackage.class.getCanonicalName() + " <version> <input file 1> [<input file 2> <input file 3> ... <input file N>]");
        System.out.println("<version> : The version of the FlowFile Package format. Valid values are 1, 2, 3");
        System.out.println("<input file X> : The FlowFile package to unpack");
        System.out.println();
    }

    public static void main(final String[] args) throws IOException {
        if (args.length < 2) {
            printUsage();
            return;
        }

        final String version = args[0];

        int inputFileCount = 0;
        int outputFileCount = 0;

        for (int i = 1; i < args.length; i++) {
            final String filename = args[i];
            final File inFile = new File(filename);

            if (inFile.isDirectory()) {
                System.out.println("WARNING: input file " + inFile + " is a directory; skipping");
                continue;
            }

            if (!inFile.exists() || !inFile.canRead()) {
                System.out.println("ERROR: unable to read file " + inFile);
                continue;
            }

            final File outputDir = new File(inFile.getAbsolutePath() + ".unpacked");
            if (!outputDir.exists() && !outputDir.mkdir()) {
                System.out.println("ERROR: Unable to create directory " + outputDir);
                continue;
            }

            final File tempFile = new File(outputDir, ".temp." + UUID.randomUUID().toString() + ".unpackage");
            inputFileCount++;
            try (final FileInputStream fis = new FileInputStream(inFile);
                    final BufferedInputStream bufferedIn = new BufferedInputStream(fis)) {

                final FlowFileUnpackager unpackager = createUnpackager(version);
                while (unpackager.hasMoreData()) {
                    outputFileCount++;
                    final Map<String, String> attributes;

                    try (final FileOutputStream fos = new FileOutputStream(tempFile);
                            final BufferedOutputStream bufferedOut = new BufferedOutputStream(fos)) {
                        attributes = unpackager.unpackageFlowFile(bufferedIn, bufferedOut);
                    }

                    String outputFilename = attributes.get("filename");
                    if (outputFilename == null) {
                        outputFilename = attributes.get("nf.file.name");
                    }

                    final File outputFile = new File(outputDir, outputFilename);
                    tempFile.renameTo(outputFile);

                    final File attributeFilename = new File(outputDir, outputFilename + ".attributes");
                    try (final FileOutputStream fos = new FileOutputStream(attributeFilename);
                            final BufferedOutputStream bufferedOut = new BufferedOutputStream(fos)) {

                        for (final Map.Entry<String, String> entry : attributes.entrySet()) {
                            bufferedOut.write((entry.getKey() + "=" + entry.getValue() + "\n").getBytes("UTF-8"));
                        }
                    }
                }
            }
        }

        System.out.println("Unpacked " + inputFileCount + " packages into " + outputFileCount + " files");
    }

    public static FlowFileUnpackager createUnpackager(final String version) {
        switch (version) {
            case "1":
                return new FlowFileUnpackagerV1();
            case "2":
                return new FlowFileUnpackagerV2();
            case "3":
                return new FlowFileUnpackagerV3();
            default:
                System.out.println("ERROR: Invalid version: " + version + "; must be 1, 2, or 3");
                return null;
        }
    }
}
