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
package org.apache.nifi.web;

import java.util.Collection;

import org.apache.nifi.controller.ControllerService;

/**
 * NiFi web context providing limited access to dataflow configuration for
 * processor custom UIs.
 */
@Deprecated
public interface NiFiWebContext {

    /**
     * @param serviceIdentifier identifier of the service
     * @return the ControllerService for the specified identifier. If a
     * corresponding service cannot be found, null is returned. If this NiFi is
     * clustered, the ControllerService is loaded from the NCM
     */
    ControllerService getControllerService(String serviceIdentifier);

    /**
     * Provides a mechanism for custom UIs to save actions to appear in NiFi
     * configuration history. Note all fields within each Action must be
     * populated. Null values will result in a failure to insert the audit
     * record. Since the saving to these actions is separate from the actual
     * configuration change, a failure to insert here will just generate a
     * warning log message. The recording of these actions typically happens
     * after a configuration change is applied. Since those changes have already
     * been applied to the flow, we cannot revert them because of a failure to
     * insert an audit record.
     *
     * @param actions to save
     */
    void saveActions(Collection<ProcessorConfigurationAction> actions);

    /**
     * @return the current user dn. Returns null if no user is found
     */
    String getCurrentUserDn();

    /**
     * @return the current user name. Returns null if no user is found
     */
    String getCurrentUserName();

    /**
     * Gets the Processor configuration. The given configuration is expected to
     * contain the following configuration:
     *
     * <ul>
     * <li>revision -- the client identifier and optionally the version
     * number</li>
     * <li>processorId -- the id of the processor to retrieve information
     * for</li>
     * <li>X509Certificate -- the certificate if this is a secure request</li>
     * </ul>
     *
     * When operating in a clustered environment, if the configuration contains
     * a X509Certificate, then the certificate information will be forwarded to
     * the nodes.
     *
     * @param config the configuration
     * @return the processor info object
     * @throws ResourceNotFoundException if the processor does not exit
     * @throws ClusterRequestException if the processor was unable to be
     * retrieved from the cluster. This exception will only be thrown when
     * operating in a cluster.
     */
    ProcessorInfo getProcessor(NiFiWebContextConfig config) throws ResourceNotFoundException, ClusterRequestException;

    /**
     * Sets the Processor annotation data. The given configuration is expected
     * to contain the following configuration:
     *
     * <ul>
     * <li>revision -- the client identifier and optionally the version
     * number</li>
     * <li>processorId -- the id of the processor to retrieve information
     * for</li>
     * <li>X509Certificate -- the certificate if this is a secure request</li>
     * </ul>
     *
     * When operating in a clustered environment, if the configuration contains
     * a X509Certificate, then the certificate information will be forwarded to
     * the nodes.
     *
     * @param config the configuration
     * @param annotationData the annotation data
     * @throws ResourceNotFoundException if the processor does not exit
     * @throws InvalidRevisionException if a revision other than the current
     * revision is given
     * @throws ClusterRequestException if the annotation data was unable to be
     * set for the processor. This exception will only be thrown when operating
     * in a cluster.
     */
    void setProcessorAnnotationData(NiFiWebContextConfig config, String annotationData)
            throws ResourceNotFoundException, InvalidRevisionException, ClusterRequestException;

}
