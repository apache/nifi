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
import org.apache.nifi.controller.AbstractControllerService;
import org.apache.nifi.controller.ControllerService;

import java.util.List;

public class PrimaryService extends AbstractControllerService {

    public static final PropertyDescriptor SECONDARY_SERVICE_ENABLED = new PropertyDescriptor.Builder()
            .name("Secondary Service Enabled")
            .required(true)
            .allowableValues(Boolean.TRUE.toString(), Boolean.FALSE.toString())
            .build();

    public static final PropertyDescriptor SECONDARY_SERVICE = new PropertyDescriptor.Builder()
            .name("Secondary Service")
            .identifiesControllerService(ControllerService.class)
            .required(true)
            .dependsOn(SECONDARY_SERVICE_ENABLED, Boolean.TRUE.toString())
            .build();

    private static final List<PropertyDescriptor> PROPERTY_DESCRIPTORS = List.of(
            SECONDARY_SERVICE_ENABLED,
            SECONDARY_SERVICE
    );

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return PROPERTY_DESCRIPTORS;
    }
}
