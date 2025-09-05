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

package org.apache.nifi.controller.serialization;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.module.jakarta.xmlbind.JakartaXmlBindAnnotationIntrospector;
import org.apache.nifi.controller.FlowController;
import org.apache.nifi.controller.flow.VersionedDataflow;
import org.apache.nifi.encrypt.PropertyEncryptor;
import org.apache.nifi.nar.ExtensionManager;

import java.io.IOException;
import java.io.OutputStream;

public class VersionedFlowSerializer implements FlowSerializer<VersionedDataflow> {
    private static final ObjectMapper JSON_CODEC = new ObjectMapper();
    private final ExtensionManager extensionManager;

    static {
        JSON_CODEC.setDefaultPropertyInclusion(JsonInclude.Include.NON_NULL);
        JSON_CODEC.setAnnotationIntrospector(new JakartaXmlBindAnnotationIntrospector(JSON_CODEC.getTypeFactory()));
        JSON_CODEC.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
    }

    public VersionedFlowSerializer(final ExtensionManager extensionManager) {
        this.extensionManager = extensionManager;
    }

    @Override
    public VersionedDataflow transform(final FlowController controller, final ScheduledStateLookup stateLookup) throws FlowSerializationException {
        final PropertyEncryptor encryptor = controller.getEncryptor();
        final VersionedDataflowMapper dataflowMapper = new VersionedDataflowMapper(controller, extensionManager, encryptor::encrypt, stateLookup);
        final VersionedDataflow dataflow = dataflowMapper.createMapping();
        return dataflow;
    }

    @Override
    public void serialize(final VersionedDataflow flowConfiguration, final OutputStream out) throws FlowSerializationException {
        try {
            final JsonFactory factory = new JsonFactory();
            final JsonGenerator generator = factory.createGenerator(out);
            generator.setCodec(JSON_CODEC);
            generator.writeObject(flowConfiguration);
            generator.flush();
        } catch (final IOException ioe) {
            throw new FlowSerializationException("Failed to write dataflow", ioe);
        }
    }
}
