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
package org.apache.nifi.controller.service.mock;

import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.controller.ControllerService;
import org.apache.nifi.reporting.AbstractReportingTask;
import org.apache.nifi.reporting.ReportingContext;

import java.util.ArrayList;
import java.util.List;

public class DummyReportingTask extends AbstractReportingTask {

    public static final PropertyDescriptor SERVICE = new PropertyDescriptor.Builder()
            .name("Controller Service")
            .identifiesControllerService(ControllerService.class)
            .required(true)
            .build();

    public static final PropertyDescriptor PROP_FOO = new PropertyDescriptor.Builder()
            .name("Foo")
            .required(false)
            .build();

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        final List<PropertyDescriptor> descriptors = new ArrayList<>();
        descriptors.add(SERVICE);
        descriptors.add(PROP_FOO);
        return descriptors;
    }

    @Override
    public void onTrigger(ReportingContext context) {

    }
}
