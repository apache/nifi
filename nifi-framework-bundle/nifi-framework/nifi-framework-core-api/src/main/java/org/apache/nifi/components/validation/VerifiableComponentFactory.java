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
package org.apache.nifi.components.validation;

import org.apache.nifi.controller.ProcessorNode;
import org.apache.nifi.controller.VerifiableControllerService;
import org.apache.nifi.controller.exception.ControllerServiceInstantiationException;
import org.apache.nifi.controller.exception.ProcessorInstantiationException;
import org.apache.nifi.controller.service.ControllerServiceNode;
import org.apache.nifi.processor.VerifiableProcessor;

public interface VerifiableComponentFactory {

    /**
     * Returns an instance with a new <code>ClassLoader</code> based on the existing <code>ProcessorNode</code>
     *
     * @param processorNode the ProcessorNode the new instance is based on
     * @param classLoader the new classloader
     * @return the verifiable processor created with new ClassLoader
     * @throws ProcessorInstantiationException if unable to create the instance
     */
    VerifiableProcessor createProcessor(ProcessorNode processorNode, ClassLoader classLoader) throws ProcessorInstantiationException;

    /**
     * Returns an instance with a new <code>ClassLoader</code> based on the existing <code>ControllerServiceNode</code>
     *
     * @param serviceNode the ControllerServiceNode the new instance is based on
     * @param classLoader the new classloader
     * @return the verifiable controller service created with the new ClassLoader
     * @throws ControllerServiceInstantiationException  if unable to create the instance
     */
    VerifiableControllerService createControllerService(ControllerServiceNode serviceNode, ClassLoader classLoader);

}
