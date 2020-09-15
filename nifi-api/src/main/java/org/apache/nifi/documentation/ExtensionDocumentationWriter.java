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
package org.apache.nifi.documentation;

import java.io.IOException;
import java.util.Collection;
import java.util.Map;
import org.apache.nifi.components.ConfigurableComponent;

/**
 * Generates documentation for an instance of a ConfigurableComponent.
 *
 * Please note that while this class lives within the nifi-api, it is provided primarily as a means for documentation components within
 * the NiFi NAR Maven Plugin. Its home is the nifi-api, however, because the API is needed in order to extract the relevant information and
 * the NAR Maven Plugin cannot have a direct dependency on nifi-api (doing so would cause a circular dependency). By having this homed within
 * the nifi-api, the Maven plugin is able to discover the class dynamically and invoke the one or two methods necessary to create the documentation.
 *
 * This is a new capability in 1.9.0 in preparation for the Extension Registry and therefore, you should
 * <b>NOTE WELL:</b> At this time, while this class is part of nifi-api, it is still evolving and may change in a non-backward-compatible manner or even be
 * removed from one incremental release to the next. Use at your own risk!
 */
public interface ExtensionDocumentationWriter {

    /**
     * Calls initialize on the component. Must be called before calling any write methods.
     *
     * @param component the component to initialize
     */
    void initialize(final ConfigurableComponent component);

    /**
     * Write the documentation for the given component.
     *
     * @param component the component to document
     * @throws IOException if an error occurs writing the documentation
     */
    void write(ConfigurableComponent component) throws IOException;

    /**
     * Writes the documentation for the given component.
     *
     * @param component the component to document
     * @param provideServices the service APIs implemented by the component
     * @param propertyServiceAPIs the service APIs required by the property descriptors of the component
     * @throws IOException if an error occurs writing the documentation
     */
    void write(ConfigurableComponent component, Collection<ServiceAPI> provideServices, Map<String,ServiceAPI> propertyServiceAPIs) throws IOException;

}
