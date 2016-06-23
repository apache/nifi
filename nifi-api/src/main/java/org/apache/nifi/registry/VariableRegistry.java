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
package org.apache.nifi.registry;


import java.util.Map;
import java.util.Set;

/**
 *  Access key/value pairs throughout the application.
 */
public interface VariableRegistry {

    /**
     * Returns a map of key/value pairs stored in the registry
     *  @return variables
     **/
    Map<String, String> getVariables();

    /**
     * Return a value for a given variable
     * @param variable variable
     * @return value
     **/
    String getVariableValue(String variable);

    /**
     * Concatenate a variable registry
     * @param variableRegistry variableRegistry
     * */
    void addRegistry(VariableRegistry variableRegistry);

    /**
     * Returns a set variable names in the registry
     * @return variableNames
     **/
    Set<String> getVariableNames();

    /**
     * Concatenate variable key value pair to registry
     * @param variables variable Map
     * */
    void addVariables(Map<String,String> variables);


}
