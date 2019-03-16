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



package org.apache.nifi.testharness.util;


import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.util.zip.GZIPOutputStream;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

public final class Zip {

    private Zip() {
        // no external instances allowed
    }


    public interface StatusListener {
        void onUncompressStarted(ZipEntry ze);

        void onUncompressDone(ZipEntry ze);
    }

    public static class StatusListenerAdapter implements StatusListener {

        @Override
        public void onUncompressStarted(ZipEntry ze) {

        }

        @Override
        public void onUncompressDone(ZipEntry ze) {

        }
    }

    private static final StatusListener NO_OP_STATUS_LISTENER = new StatusListenerAdapter();

    public static void unzipFile(File zipFile, File targetDirectory) throws IOException {
        unzipFile(zipFile, targetDirectory, NO_OP_STATUS_LISTENER);

    }


    public static void unzipFile(File zipFile, File targetDirectory,
                                 StatusListener statusListener) throws IOException {

        if (!targetDirectory.exists()) {
            boolean mkdirs = targetDirectory.mkdirs();
            if (!mkdirs) {
                throw new IOException("Failed to create directory: " + targetDirectory);
            }
        }

        try (ZipInputStream zipInputStream = new ZipInputStream(new FileInputStream(zipFile))) {

            ZipEntry ze = zipInputStream.getNextEntry();

            while (ze != null) {

                if(ze.isDirectory()) {
                    ze = zipInputStream.getNextEntry();
                    continue;
                }

                statusListener.onUncompressStarted(ze);

                String fileName = ze.getName();
                File outputFile = new File(targetDirectory, fileName);


                File parentDir = new File(outputFile.getParent());
                if (!parentDir.exists()) {
                    boolean couldCreateParentDir = parentDir.mkdirs();
                    if (!couldCreateParentDir) {
                        throw new IllegalStateException("Could not create: " + parentDir);

                    }
                }



                Files.copy(zipInputStream, outputFile.toPath());

                statusListener.onUncompressDone(ze);


                ze = zipInputStream.getNextEntry();
            }

            zipInputStream.closeEntry();


        }


    }


    public static void gzipFile(File inputFile, File gzipFile) throws IOException {

        try (GZIPOutputStream gzos =
                     new GZIPOutputStream(new FileOutputStream(gzipFile))) {


            Files.copy(inputFile.toPath(), gzos);

            gzos.finish();
        }
    }


}
