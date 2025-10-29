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
package org.apache.nifi.processors.box;

import org.apache.nifi.box.controllerservices.BoxClientService;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.migration.PropertyConfiguration;
import org.apache.nifi.processor.AbstractProcessor;

abstract class AbstractBoxProcessor extends AbstractProcessor {
    static final String OLD_BOX_CLIENT_SERVICE_PROPERTY_NAME = "box-client-service";

    static final PropertyDescriptor BOX_CLIENT_SERVICE = new PropertyDescriptor.Builder()
            .name("Box Client Service")
            .description("Controller Service used to obtain a Box API connection.")
            .identifiesControllerService(BoxClientService.class)
            .required(true)
            .build();

    @Override
    public void migrateProperties(PropertyConfiguration config) {
        config.renameProperty(OLD_BOX_CLIENT_SERVICE_PROPERTY_NAME, BOX_CLIENT_SERVICE.getName());
    }
}
