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

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

public class AggregateNiFiInstance implements NiFiInstance {
    private final List<NiFiInstance> instances;

    public AggregateNiFiInstance(final List<NiFiInstance> instances) {
        this.instances = instances;
    }

    @Override
    public void start(boolean waitForCompletion) {
        final Map<Thread, NiFiInstance> startupThreads = new HashMap<>();

        for (final NiFiInstance instance : instances) {
            if (instance.isAutoStart()) {
                final Thread t = new Thread(() -> instance.start(waitForCompletion));
                t.start();
                startupThreads.put(t, instance);
            }
        }

        for (final Map.Entry<Thread, NiFiInstance> entry : startupThreads.entrySet()) {
            final Thread startupThread = entry.getKey();

            try {
                startupThread.join();
            } catch (final InterruptedException ie) {
                throw new RuntimeException("Interrupted while waiting for instance " + entry.getValue() + " to finish starting");
            }
        }
    }

    @Override
    public void createEnvironment() throws IOException {
        for (final NiFiInstance instance : instances) {
            instance.createEnvironment();
        }
    }

    @Override
    public void stop() {
        Exception thrown = null;

        // Shut down in the opposite order that they were brought up. We do this because only the first instance is going to be running ZooKeeper, and we don't
        // want to kill that before the other instances are shutdown.
        for (int i=instances.size() - 1; i >= 0; i--) {
            final NiFiInstance instance = instances.get(i);

            try {
                instance.stop();
            } catch (final Exception e) {
                thrown = e;
            }
        }

        if (thrown != null) {
            throw (RuntimeException) thrown;
        }
    }


    @Override
    public boolean isClustered() {
        return true;
    }

    @Override
    public int getNumberOfNodes() {
        return instances.size();
    }

    @Override
    public int getNumberOfNodes(final boolean includeOnlyAutoStartInstances) {
        if (includeOnlyAutoStartInstances) {
            return (int) instances.stream()
                .filter(NiFiInstance::isAutoStart)
                .count();
        }

        return instances.size();
    }

    @Override
    public NiFiInstance getNodeInstance(final int nodeIndex) {
        if (nodeIndex < 1 || nodeIndex > instances.size()) {
            throw new IllegalArgumentException("Node Index must be between 1 and " + instances.size() + "; invalid value given: " + nodeIndex);
        }

        return instances.get(nodeIndex - 1);
    }

    @Override
    public Properties getProperties() {
        return null;
    }

    @Override
    public File getInstanceDirectory() {
        return null;
    }

    @Override
    public boolean isAutoStart() {
        return true;
    }

    @Override
    public void setProperty(final String propertyName, final String propertyValue) throws IOException {
        for (final NiFiInstance instance : instances) {
            instance.setProperty(propertyName, propertyValue);
        }
    }

    @Override
    public void setFlowXmlGz(final File flowXmlGz) throws IOException {
        for (final NiFiInstance instance : instances) {
            instance.setFlowXmlGz(flowXmlGz);
        }
    }

    @Override
    public void setProperties(final Map<String, String> properties) throws IOException {
        for (final NiFiInstance instance : instances) {
            instance.setProperties(properties);
        }
    }
}
