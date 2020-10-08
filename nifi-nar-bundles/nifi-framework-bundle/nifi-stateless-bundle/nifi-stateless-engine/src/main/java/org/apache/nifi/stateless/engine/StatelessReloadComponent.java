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

package org.apache.nifi.stateless.engine;

import org.apache.nifi.bundle.BundleCoordinate;
import org.apache.nifi.controller.ProcessorNode;
import org.apache.nifi.controller.ReloadComponent;
import org.apache.nifi.controller.ReportingTaskNode;
import org.apache.nifi.controller.exception.ControllerServiceInstantiationException;
import org.apache.nifi.controller.exception.ProcessorInstantiationException;
import org.apache.nifi.controller.reporting.ReportingTaskInstantiationException;
import org.apache.nifi.controller.service.ControllerServiceNode;

import java.net.URL;
import java.util.Set;

public class StatelessReloadComponent implements ReloadComponent {
    @Override
    public void reload(final ProcessorNode existingNode, final String newType, final BundleCoordinate bundleCoordinate, final Set<URL> additionalUrls) throws ProcessorInstantiationException {

    }

    @Override
    public void reload(final ControllerServiceNode existingNode, final String newType, final BundleCoordinate bundleCoordinate, final Set<URL> additionalUrls)
        throws ControllerServiceInstantiationException {

    }

    @Override
    public void reload(final ReportingTaskNode existingNode, final String newType, final BundleCoordinate bundleCoordinate, final Set<URL> additionalUrls) throws ReportingTaskInstantiationException {

    }
}
