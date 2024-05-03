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

import org.apache.nifi.controller.repository.metrics.RingBufferEventRepository;
import org.springframework.beans.factory.FactoryBean;

public class RingBufferEventRepositoryBean implements FactoryBean<RingBufferEventRepository> {

    private RingBufferEventRepository repository;

    @Override
    public RingBufferEventRepository getObject() throws Exception {
        if (repository == null) {
            // create the h2 repository
            repository = new RingBufferEventRepository(5);
        }
        return repository;
    }

    @Override
    public Class<?> getObjectType() {
        return RingBufferEventRepository.class;
    }

    @Override
    public boolean isSingleton() {
        return true;
    }
}
