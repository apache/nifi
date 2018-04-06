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
package org.apache.nifi.pulsar;

import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.controller.ControllerService;
import org.apache.nifi.pulsar.pool.ResourcePool;


@Tags({"Pulsar"})
@CapabilityDescription("Provides the ability to create Pulsar Producer / Consumer instances on demand, based on the configuration."
                     + "properties defined")
/**
 * Service definition for apache Pulsar Client ControllerService
 * responsible for maintaining a pool of @PulsarProducer and
 * @PulsarConsumer objects.
 *
 * Since both of these objects can be reused, in a manner similar
 * to database connections, and the cost to create these objects is
 * relatively high. The PulsarClientPool keeps these objects in pools
 * for re-use.
 *
 * @author david
 *
 */
public interface PulsarClientPool extends ControllerService {

    /**
     * Returns the pool of @PulsarProducer objects.
     * @return ResourcePool
     */
    public ResourcePool<PulsarProducer> getProducerPool();

    /**
     * Returns the pool of @PulsarConsumer objects.
     * @return ResourcePool
     */
    public ResourcePool<PulsarConsumer> getConsumerPool();
}
