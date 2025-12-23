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

import org.apache.nifi.controller.ControllerService;
import org.apache.nifi.controller.ControllerServiceInitializationContext;
import org.apache.nifi.controller.FlowController;
import org.apache.nifi.controller.ProcessorNode;
import org.apache.nifi.controller.VerifiableControllerService;
import org.apache.nifi.controller.exception.ControllerServiceInstantiationException;
import org.apache.nifi.controller.exception.ProcessorInstantiationException;
import org.apache.nifi.controller.service.ControllerServiceNode;
import org.apache.nifi.controller.service.StandardControllerServiceInitializationContext;
import org.apache.nifi.processor.Processor;
import org.apache.nifi.processor.ProcessorInitializationContext;
import org.apache.nifi.processor.StandardProcessorInitializationContext;
import org.apache.nifi.processor.VerifiableProcessor;
import org.apache.nifi.util.NiFiProperties;

public class StandardVerifiableComponentFactory implements VerifiableComponentFactory {

    private final FlowController flowController;
    private final NiFiProperties nifiProperties;
    public StandardVerifiableComponentFactory(final FlowController flowController, final NiFiProperties nifiProperties) {
        this.flowController = flowController;
        this.nifiProperties = nifiProperties;
    }

    @Override
    public VerifiableProcessor createProcessor(final ProcessorNode processorNode, final ClassLoader classLoader) throws ProcessorInstantiationException {
        final VerifiableProcessor verifiableProcessor;
        final String identifier = processorNode.getIdentifier();
        final String processorClassName = processorNode.getProcessor().getClass().getName();
        try {
            final Class<?> rawProcessorClass = Class.forName(processorClassName, true, classLoader);
            final Class<? extends VerifiableProcessor> processorClass = rawProcessorClass.asSubclass(VerifiableProcessor.class);
            verifiableProcessor = processorClass.getDeclaredConstructor().newInstance();

            final ProcessorInitializationContext tempInitializationContext = new StandardProcessorInitializationContext(identifier, processorNode.getLogger(),
                    flowController.getControllerServiceProvider(), flowController, flowController.createKerberosConfig(nifiProperties));
            if (verifiableProcessor instanceof Processor processor) {
                processor.initialize(tempInitializationContext);
            }
        } catch (Exception e) {
            throw new ProcessorInstantiationException("Failed to instantiate Verifiable Processor Class [%s]".formatted(processorClassName), e);
        }
        return verifiableProcessor;
    }

    @Override
    public VerifiableControllerService createControllerService(final ControllerServiceNode serviceNode, final ClassLoader classLoader) {
        final VerifiableControllerService verifiableControllerService;
        final String identifier = serviceNode.getIdentifier();
        final String controllerServiceClassName = serviceNode.getCanonicalClassName();
        try {
            final Class<?> rawControllorServiceClass = Class.forName(controllerServiceClassName, true, classLoader);
            final Class<? extends VerifiableControllerService> controllerServiceClass = rawControllorServiceClass.asSubclass(VerifiableControllerService.class);
            verifiableControllerService = controllerServiceClass.getDeclaredConstructor().newInstance();

            final ControllerServiceInitializationContext tempInitializationContext = new StandardControllerServiceInitializationContext(identifier,
                    serviceNode.getLogger(),
                    flowController.getControllerServiceProvider(), flowController.getStateManagerProvider().getStateManager(identifier),
                    flowController.createKerberosConfig(nifiProperties), flowController);
            if (verifiableControllerService instanceof ControllerService controllerService) {
                controllerService.initialize(tempInitializationContext);
            }
        } catch (Exception e) {
            throw new ControllerServiceInstantiationException("Failed to instantiate Verifiable Controller Service Class [%s]".formatted(controllerServiceClassName), e);
        }
        return verifiableControllerService;
    }

}
