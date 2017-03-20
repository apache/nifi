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
package org.apache.nifi.controller.serialization;

import org.apache.nifi.cluster.protocol.DataFlow;
import org.apache.nifi.controller.FlowController;
import org.apache.nifi.controller.MissingBundleException;
import org.apache.nifi.controller.UninheritableFlowException;
import org.apache.nifi.encrypt.StringEncryptor;

/**
 */
public interface FlowSynchronizer {

    /**
     * Synchronizes the given controller with the given flow configuration. If loading the proposed flow configuration would cause the controller to orphan flow files, then an
     * UninheritableFlowException is thrown.
     *
     * If the FlowSynchronizationException is thrown, then the controller may have changed some of its state and should no longer be used.
     *
     * @param controller the flow controller
     * @param dataFlow the flow to load the controller with. If the flow is null or zero length, then the controller must not have a flow or else an UninheritableFlowException will be thrown.
     * @param encryptor used for the encryption/decryption of sensitive property values
     *
     * @throws FlowSerializationException if proposed flow is not a valid flow configuration file
     * @throws UninheritableFlowException if the proposed flow cannot be loaded by the controller because in doing so would risk orphaning flow files
     * @throws FlowSynchronizationException if updates to the controller failed. If this exception is thrown, then the controller should be considered unsafe to be used
     * @throws MissingBundleException if the proposed flow cannot be loaded by the controller because it contains a bundle that is not available to the controller
     */
    void sync(FlowController controller, DataFlow dataFlow, StringEncryptor encryptor)
            throws FlowSerializationException, UninheritableFlowException, FlowSynchronizationException, MissingBundleException;

}
