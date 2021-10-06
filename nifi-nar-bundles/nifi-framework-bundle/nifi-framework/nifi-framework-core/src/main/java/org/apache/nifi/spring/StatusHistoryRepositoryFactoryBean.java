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
package org.apache.nifi.spring;

import org.apache.nifi.controller.status.history.StatusHistoryRepository;
import org.apache.nifi.controller.status.history.StatusHistoryRepositoryFactory;
import org.apache.nifi.nar.ExtensionManager;
import org.apache.nifi.util.NiFiProperties;
import org.springframework.beans.factory.FactoryBean;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;

/**
 * Factory bean for creating a singleton StatusHistoryRepository instance.
 */
public class StatusHistoryRepositoryFactoryBean implements FactoryBean<StatusHistoryRepository>, ApplicationContextAware {


    private NiFiProperties nifiProperties;
    private ExtensionManager extensionManager;

    @Override
    public StatusHistoryRepository getObject() throws Exception {
        return StatusHistoryRepositoryFactory.createStatusHistoryRepositoryFactory(nifiProperties, extensionManager);
    }

    @Override
    public Class<?> getObjectType() {
        return StatusHistoryRepository.class;
    }

    @Override
    public void setApplicationContext(ApplicationContext applicationContext) {
        // This class uses setter injection instead of ApplicationContext.
    }

    public void setNifiProperties(NiFiProperties nifiProperties) {
        this.nifiProperties = nifiProperties;
    }

    public void setExtensionManager(ExtensionManager extensionManager) {
        this.extensionManager = extensionManager;
    }
}
