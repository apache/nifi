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
package org.apache.nifi.stateless.core.compatibility;

import org.apache.nifi.registry.flow.VersionedProcessor;

import java.util.Map;
import java.util.Set;

public class StatelessVersionedProcessor implements StatelessProcessor {

    private final VersionedProcessor processor;

    public StatelessVersionedProcessor(final VersionedProcessor processor) {
        this.processor = processor;
    }

    @Override
    public String getId() {
        return this.processor.getIdentifier();
    }

    @Override
    public Map<String, String> getProperties() {
        return this.processor.getProperties();
    }

    @Override
    public String getAnnotationData() {
        return this.processor.getAnnotationData();
    }

    @Override
    public Set<String> getAutoTerminatedRelationships() {
        return this.processor.getAutoTerminatedRelationships();
    }

    @Override
    public String getType() {
        return this.processor.getType();
    }

    @Override
    public String getBundleGroup() {
        return this.processor.getBundle().getGroup();
    }

    @Override
    public String getBundleArtifact() {
        return this.processor.getBundle().getArtifact();
    }

    @Override
    public String getBundleVersion() {
        return this.processor.getBundle().getVersion();
    }
}
