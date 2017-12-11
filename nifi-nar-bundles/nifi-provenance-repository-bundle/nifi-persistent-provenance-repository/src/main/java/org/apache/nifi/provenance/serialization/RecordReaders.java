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

import java.io.BufferedInputStream;
import java.io.DataInputStream;
import java.io.EOFException;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Path;
import java.util.Collection;
import java.util.zip.GZIPInputStream;
import org.apache.nifi.properties.NiFiPropertiesLoader;
import org.apache.nifi.provenance.ByteArraySchemaRecordReader;
import org.apache.nifi.provenance.ByteArraySchemaRecordWriter;
import org.apache.nifi.provenance.EncryptedSchemaRecordReader;
import org.apache.nifi.provenance.EventIdFirstSchemaRecordReader;
import org.apache.nifi.provenance.EventIdFirstSchemaRecordWriter;
import org.apache.nifi.provenance.StandardRecordReader;
import org.apache.nifi.provenance.lucene.LuceneUtil;
import org.apache.nifi.provenance.toc.StandardTocReader;
import org.apache.nifi.provenance.toc.TocReader;
import org.apache.nifi.provenance.toc.TocUtil;
import org.apache.nifi.security.kms.CryptoUtils;
import org.apache.nifi.util.NiFiProperties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RecordReaders {

    private static Logger logger = LoggerFactory.getLogger(RecordReaders.class);

    private static boolean isEncryptionAvailable = false;
    private static boolean encryptionPropertiesRead = false;

    /**
     * Creates a new Record Reader that is capable of reading Provenance Event Journals
     *
     * @param file               the Provenance Event Journal to read data from
     * @param provenanceLogFiles collection of all provenance journal files
     * @param maxAttributeChars  the maximum number of characters to retrieve for any one attribute. This allows us to avoid
     *                           issues where a FlowFile has an extremely large attribute and reading events
     *                           for that FlowFile results in loading that attribute into memory many times, exhausting the Java Heap
     * @return a Record Reader capable of reading Provenance Event Journals
     * @throws IOException if unable to create a Record Reader for the given file
     */
    public static RecordReader newRecordReader(File file, final Collection<Path> provenanceLogFiles, final int maxAttributeChars) throws IOException {
        final File originalFile = file;
        InputStream fis = null;

        try {
            if (!file.exists()) {
                if (provenanceLogFiles != null) {
                    final String baseName = LuceneUtil.substringBefore(file.getName(), ".") + ".";
                    for (final Path path : provenanceLogFiles) {
                        if (path.toFile().getName().startsWith(baseName)) {
                            file = path.toFile();
                            break;
                        }
                    }
                }
            }

            if (file.exists()) {
                try {
                    fis = new FileInputStream(file);
                } catch (final FileNotFoundException fnfe) {
                    fis = null;
                }
            }

            String filename = file.getName();
            openStream:
            while (fis == null) {
                final File dir = file.getParentFile();
                final String baseName = LuceneUtil.substringBefore(file.getName(), ".prov");

                // depending on which rollover actions have occurred, we could have 2 possibilities for the
                // filename that we need. The majority of the time, we will use the extension ".prov.gz"
                // because most often we are compressing on rollover and most often we have already finished
                // compressing by the time that we are querying the data.
                for (final String extension : new String[]{".prov.gz", ".prov"}) {
                    file = new File(dir, baseName + extension);
                    if (file.exists()) {
                        try {
                            fis = new FileInputStream(file);
                            filename = baseName + extension;
                            break openStream;
                        } catch (final FileNotFoundException fnfe) {
                            // file was modified by a RolloverAction after we verified that it exists but before we could
                            // create an InputStream for it. Start over.
                            fis = null;
                            continue openStream;
                        }
                    }
                }

                break;
            }

            if (fis == null) {
                throw new FileNotFoundException("Unable to locate file " + originalFile);
            }

            final File tocFile = TocUtil.getTocFile(file);

            final InputStream bufferedInStream = new BufferedInputStream(fis);
            final String serializationName;
            try {
                bufferedInStream.mark(4096);
                final InputStream in = filename.endsWith(".gz") ? new GZIPInputStream(bufferedInStream) : bufferedInStream;
                final DataInputStream dis = new DataInputStream(in);
                serializationName = dis.readUTF();
                bufferedInStream.reset();
            } catch (final EOFException eof) {
                fis.close();
                return new EmptyRecordReader();
            }

            switch (serializationName) {
                case StandardRecordReader.SERIALIZATION_NAME: {
                    if (tocFile.exists()) {
                        final TocReader tocReader = new StandardTocReader(tocFile);
                        return new StandardRecordReader(bufferedInStream, filename, tocReader, maxAttributeChars);
                    } else {
                        return new StandardRecordReader(bufferedInStream, filename, maxAttributeChars);
                    }
                }
                case ByteArraySchemaRecordWriter.SERIALIZATION_NAME: {
                    if (tocFile.exists()) {
                        final TocReader tocReader = new StandardTocReader(tocFile);
                        return new ByteArraySchemaRecordReader(bufferedInStream, filename, tocReader, maxAttributeChars);
                    } else {
                        return new ByteArraySchemaRecordReader(bufferedInStream, filename, maxAttributeChars);
                    }
                }
                case EventIdFirstSchemaRecordWriter.SERIALIZATION_NAME: {
                    if (!tocFile.exists()) {
                        throw new FileNotFoundException("Cannot create TOC Reader because the file " + tocFile + " does not exist");
                    }

                    final TocReader tocReader = new StandardTocReader(tocFile);
                    return new EventIdFirstSchemaRecordReader(bufferedInStream, filename, tocReader, maxAttributeChars);
                }
                case EncryptedSchemaRecordReader.SERIALIZATION_NAME: {
                    if (!tocFile.exists()) {
                        throw new FileNotFoundException("Cannot create TOC Reader because the file " + tocFile + " does not exist");
                    }

                    if (!isEncryptionAvailable()) {
                        throw new IOException("Cannot read encrypted repository because this reader is not configured for encryption");
                    }

                    final TocReader tocReader = new StandardTocReader(tocFile);
                    // Return a reader with no eventEncryptor because this method contract cannot change, then inject the encryptor from the writer in the calling method
                    return new EncryptedSchemaRecordReader(bufferedInStream, filename, tocReader, maxAttributeChars, null);
                }
                default: {
                    throw new IOException("Unable to read data from file " + file + " because the file was written using an unknown Serializer: " + serializationName);
                }
            }
        } catch (final IOException ioe) {
            if (fis != null) {
                try {
                    fis.close();
                } catch (final IOException inner) {
                    ioe.addSuppressed(inner);
                }
            }

            throw ioe;
        }
    }

    private static boolean isEncryptionAvailable() {
        if (encryptionPropertiesRead) {
            return isEncryptionAvailable;
        } else {
            try {
                NiFiProperties niFiProperties = NiFiPropertiesLoader.loadDefaultWithKeyFromBootstrap();
                isEncryptionAvailable = CryptoUtils.isProvenanceRepositoryEncryptionConfigured(niFiProperties);
                encryptionPropertiesRead = true;
            } catch (IOException e) {
                logger.error("Encountered an error checking the provenance repository encryption configuration: ", e);
                isEncryptionAvailable = false;
            }
            return isEncryptionAvailable;
        }
    }

}
