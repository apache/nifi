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

import org.apache.nifi.web.api.dto.ProcessorDTO;

import java.util.Map;
import java.util.Set;

public class StatelessProcessorDTO implements StatelessProcessor {

    private final ProcessorDTO processor;

    public StatelessProcessorDTO(final ProcessorDTO processor) {
        this.processor = processor;
    }

    @Override
    public String getId() {
        return this.processor.getId();
    }

    @Override
    public String getType() {
        return processor.getType();
    }

    @Override
    public String getBundleGroup() {
        return processor.getBundle().getGroup();
    }

    @Override
    public String getBundleArtifact() {
        return processor.getBundle().getArtifact();
    }

    @Override
    public String getBundleVersion() {
        return processor.getBundle().getVersion();
    }

    @Override
    public Map<String, String> getProperties() {
        return processor.getConfig().getProperties();
    }

    @Override
    public String getAnnotationData() {
        return processor.getConfig().getAnnotationData();
    }

    @Override
    public Set<String> getAutoTerminatedRelationships() {
        return processor.getConfig().getAutoTerminatedRelationships();
    }
}
