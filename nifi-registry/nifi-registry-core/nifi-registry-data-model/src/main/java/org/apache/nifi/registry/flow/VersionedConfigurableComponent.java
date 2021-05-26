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
package org.apache.nifi.registry.flow;

import java.util.Map;

/**
 * A component that has property descriptors and can be configured with values for those properties.
 */
public interface VersionedConfigurableComponent {

    Map<String,VersionedPropertyDescriptor> getPropertyDescriptors();

    void setPropertyDescriptors(Map<String,VersionedPropertyDescriptor> propertyDescriptors);

    Map<String,String> getProperties();

    void setProperties(Map<String,String> properties);

}
