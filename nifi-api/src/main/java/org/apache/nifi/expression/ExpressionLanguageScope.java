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
package org.apache.nifi.expression;

/**
 * Indicates the scope of expression language on a property descriptor.
 *
 * Scope of the expression language is hierarchical.
 *      NONE -> VARIABLE_REGISTRY -> FLOWFILE_ATTRIBUTES
 *
 * When scope is set to FlowFiles attributes, variables are evaluated
 * against attributes of each incoming flow file. If no matching attribute
 * is found, variable registry will be checked.
 *
 * NONE - expression language is not supported
 *
 * VARIABLE_REGISTRY is hierarchically constructed as below:
 *  |---- Variables defined at process group level and then, recursively, up
 *  |     to the higher process group until the root process group.
 *  |--- Variables defined in custom properties files through the
 *  |    nifi.variable.registry.properties property in nifi.properties file.
 *  |-- Environment variables defined at JVM level and system properties.
 *
 * FLOWFILE_ATTRIBUTES - will check attributes of each individual flow file
 *
 */
public enum ExpressionLanguageScope {

    /**
     * Expression language is disabled
     */
    NONE("Not Supported"),

    /**
     * Expression language is evaluated against variables in registry
     */
    VARIABLE_REGISTRY("Variable Registry Only"),

    /**
     * Expression language is evaluated per flow file using attributes
     */
    FLOWFILE_ATTRIBUTES("Variable Registry and FlowFile Attributes");

    private String description;

    private ExpressionLanguageScope(String description) {
        this.description = description;
    }

    public String getDescription() {
        return this.description;
    }

}
