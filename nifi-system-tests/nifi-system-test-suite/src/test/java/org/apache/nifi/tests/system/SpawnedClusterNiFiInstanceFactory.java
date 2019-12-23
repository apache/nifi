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
package org.apache.nifi.tests.system;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class SpawnedClusterNiFiInstanceFactory implements NiFiInstanceFactory {
    private final List<InstanceConfiguration> instanceConfigs = new ArrayList<>();

    public SpawnedClusterNiFiInstanceFactory(final String... bootstrapFilenames) {
        int i = 0;
        for (final String bootstrapFilename : bootstrapFilenames) {
            final InstanceConfiguration config = new InstanceConfiguration.Builder()
                .bootstrapConfig(bootstrapFilename)
                .instanceDirectory("target/node" + (++i))
                .build();

            instanceConfigs.add(config);
        }
    }

    public SpawnedClusterNiFiInstanceFactory(final String bootstrapFilename, final int nodeIndex) {
        final InstanceConfiguration config = new InstanceConfiguration.Builder()
            .bootstrapConfig(bootstrapFilename)
            .instanceDirectory("target/node" + nodeIndex)
            .build();

        instanceConfigs.add(config);
    }

    public SpawnedClusterNiFiInstanceFactory(final InstanceConfiguration... instanceConfigurations) {
        instanceConfigs.addAll(Arrays.asList(instanceConfigurations));
    }

    @Override
    public NiFiInstance createInstance() {
        final List<NiFiInstance> instances = new ArrayList<>();

        for (final InstanceConfiguration configuration : instanceConfigs) {
            final NiFiInstance clusteredInstance = new SpawnedStandaloneNiFiInstanceFactory(configuration).createInstance();
            instances.add(clusteredInstance);
        }

        if (instances.size() == 1) {
            return instances.get(0);
        }

        return new AggregateNiFiInstance(instances);
    }
}
