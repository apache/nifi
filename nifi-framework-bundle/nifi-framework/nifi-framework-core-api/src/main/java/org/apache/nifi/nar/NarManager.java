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

import java.io.IOException;
import java.io.InputStream;
import java.util.Collection;
import java.util.Optional;

/**
 * Manages NARs that are dynamically installed/uninstalled.
 *
 * These NARs are considered separate from NARs that are provided in NiFi's standard lib and NAR directories.
 */
public interface NarManager {

    /**
     * Installs the NAR specified in the given request.
     *
     * @param installRequest the install request
     * @return the node representing the installed NAR
     * @throws IOException if an I/O error occurs reading the input stream or persisting the NAR
     */
    NarNode installNar(NarInstallRequest installRequest) throws IOException;

    /**
     * Notifies the NAR Manager that the install task has completed for the given NAR.
     *
     * @param identifier the NAR identifier
     */
    void completeInstall(String identifier);

    /**
     * Updates the state for the NAR with the given coordinate.
     *
     * Primarily used when loading one NAR causes other NARs to load and their state needs to be updated.
     *
     * @param coordinate the coordinate of the NAR
     * @param narState the new state
     */
    void updateState(BundleCoordinate coordinate, NarState narState);

    /**
     * Updates the state of the NAR with the given coordinate to be in a failed state for the given exception that caused the failure.
     *
     * @param coordinate the coordinate of the NAR
     * @param failure the exception that caused the failure
     */
    void updateFailed(BundleCoordinate coordinate, Throwable failure);

    /**
     * @return all NARs contained in the NAR Manager
     */
    Collection<NarNode> getNars();

    /**
     * Retrieves the node for the given NAR.
     *
     * @param identifier the NAR identifier
     * @return optional containing the NAR node, or empty optional
     */
    Optional<NarNode> getNar(String identifier);

    /**
     * Verifies the given NAR can be deleted.
     *
     * @param identifier the NAR identifier
     * @param forceDelete indicates if the delete should proceed even when components are instantiated from the given NAR
     */
    void verifyDeleteNar(String identifier, boolean forceDelete);

    /**
     * Deletes the given NAR.
     *
     * @param identifier the NAR identifier
     * @return the node for the deleted NAR
     * @throws IOException if an I/O error occurs deleting the NAR
     */
    NarNode deleteNar(String identifier) throws IOException;

    /**
     * Returns a stream to read the contents of the given NAR.
     *
     * @param identifier the NAR identifier
     * @return the input stream for the content of the given NAR
     */
    InputStream readNar(String identifier);

    /**
     * Instructs the NAR Manager to sync its NARs with the NARs from the cluster coordinator.
     */
    void syncWithClusterCoordinator();

}
