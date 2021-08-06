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

package org.apache.nifi.stateless.parameter;

import org.apache.nifi.components.ConfigurableComponent;

public interface ParameterProvider extends ConfigurableComponent {

    void initialize(ParameterProviderInitializationContext context);

    /**
     * Given a Parameter Context Name and a Parameter Name, returns the value of the parameter
     * @param contextName the name of the Parameter Context
     * @param parameterName the name of the Parameter
     * @return the value for the Parameter, or <code>null</code> if no value has been specified
     */
    String getParameterValue(String contextName, String parameterName);

    boolean isParameterDefined(String contextName, String parameterName);
}
