/*
 *  Licensed to the Apache Software Foundation (ASF) under one or more
 *  contributor license agreements.  See the NOTICE file distributed with
 *  this work for additional information regarding copyright ownership.
 *  The ASF licenses this file to You under the Apache License, Version 2.0
 *  (the "License"); you may not use this file except in compliance with
 *  the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.apache.nifi.nar;

import org.apache.nifi.bundle.BundleCoordinate;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.Set;

/**
 * Provides persistence for NAR files dynamically added to NiFi.
 */
public interface NarPersistenceProvider {

    /**
     * Called after instantiation to initialize the provider.
     *
     * @param initializationContext the context containing and properties and configuration required to initialize the provider
     */
    void initialize(NarPersistenceProviderInitializationContext initializationContext) throws IOException;

    /**
     * Creates a temporary file containing the contents of the given input stream.
     *
     * @param inputStream the input stream
     * @return the file containing the contents of the input stream
     *
     * @throws IOException if an I/O error occurs writing the stream to the temp file
     */
    File createTempFile(InputStream inputStream) throws IOException;

    /**
     * Saves the temporary NAR file to its final destination.
     *
     * @param persistenceContext the context information
     * @param tempNarFile the temporary file created from calling createTempFile
     * @return the information about the persisted NAR, including the saved file
     *
     * @throws IOException if an I/O error occurs saving the temp NAR file
     */
    NarPersistenceInfo saveNar(NarPersistenceContext persistenceContext, File tempNarFile) throws IOException;

    /**
     * Deletes the contents of the NAR with the given coordinate.
     *
     * @param narCoordinate the coordinate of the NAR to delete
     *
     * @throws FileNotFoundException if a NAR with the given coordinate does not exist
     * @throws IOException if an I/O error occurred deleting the NAR
     */
    void deleteNar(BundleCoordinate narCoordinate) throws IOException;

    /**
     * Obtains an input stream to read the contents of the NAR with the given coordinate.
     *
     * @param narCoordinate the coordinate of the NAR
     * @return the input stream for the NAR
     *
     * @throws FileNotFoundException if a NAR with the given coordinate does not exist
     */
    InputStream readNar(BundleCoordinate narCoordinate) throws FileNotFoundException;

    /**
     * Retrieves the persistence info for the NAR with the given NAR coordinate.
     *
     * @param narCoordinate the coordinate of the NAR
     * @return the persistence info
     * @throws IOException if an I/O error occurs reading the persistence info
     */
    NarPersistenceInfo getNarInfo(BundleCoordinate narCoordinate) throws IOException;

    /**
     * Indicates if a NAR with the given coordinate exists in this persistence provider.
     *
     * @param narCoordinate the coordinate of the NAR
     * @return true if a NAR with the coordinate exists, false otherwise
     */
    boolean exists(BundleCoordinate narCoordinate);

    /**
     * @return the info for all NARs persisted by this persistence provider
     */
    Set<NarPersistenceInfo> getAllNarInfo() throws IOException;

    /**
     * Shuts down the provider and cleans up any resources held by it. Once this method has returned, the
     * provider may be initialized once again via the {@link #initialize(NarPersistenceProviderInitializationContext)} method.
     */
    void shutdown();

}
