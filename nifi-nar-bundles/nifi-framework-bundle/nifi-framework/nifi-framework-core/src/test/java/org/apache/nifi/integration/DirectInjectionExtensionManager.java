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
package org.apache.nifi.integration;

import org.apache.nifi.bundle.Bundle;
import org.apache.nifi.bundle.BundleDetails;
import org.apache.nifi.controller.ControllerService;
import org.apache.nifi.nar.StandardExtensionDiscoveringManager;
import org.apache.nifi.nar.SystemBundle;
import org.apache.nifi.processor.Processor;
import org.apache.nifi.reporting.ReportingTask;

import java.io.File;

public class DirectInjectionExtensionManager extends StandardExtensionDiscoveringManager {
    private static final Bundle INTEGRATION_TEST_BUNDLE = new Bundle(new BundleDetails.Builder()
        .workingDir(new File("target"))
        .coordinate(SystemBundle.SYSTEM_BUNDLE_COORDINATE)
        .build(), ClassLoader.getSystemClassLoader());

    public void injectExtension(final Object extension) {
        final Class<?> extensionType;
        if (extension instanceof Processor) {
            extensionType = Processor.class;
        } else if (extension instanceof ControllerService) {
            extensionType = ControllerService.class;
        } else if (extension instanceof ReportingTask) {
            extensionType = ReportingTask.class;
        } else {
            throw new IllegalArgumentException("Given extension is not a Processor, Controller Service, or Reporting Task");
        }

        super.loadExtension(extension.getClass().getName(), extensionType, INTEGRATION_TEST_BUNDLE);
    }

    public void injectExtensionType(final Class<?> extensionType, final String implementationClassName) {
        final Class<?> implementationClass;
        try {
            implementationClass = Class.forName(implementationClassName, false, INTEGRATION_TEST_BUNDLE.getClassLoader());
        } catch (ClassNotFoundException e) {
            throw new IllegalArgumentException("Could not find implementation class: " + implementationClassName);
        }

        injectExtensionType(extensionType, implementationClass);
    }

    public void injectExtensionType(final Class<?> extensionType, final Class<?> implementationClass) {
        super.registerExtensionClass(extensionType, implementationClass.getName(), INTEGRATION_TEST_BUNDLE);
    }
}
