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
package org.apache.nifi.web;

import java.util.Map;

public class ComponentDescriptor {

    private final String name;
    private final String displayName;
    private final String description;
    private final String defaultValue;
    private final Map<String,String> allowableValues;

    private ComponentDescriptor(Builder builder){
        this.name = builder.name;
        this.displayName = builder.displayName;
        this.description = builder.description;
        this.defaultValue = builder.defaultValue;
        this.allowableValues = builder.allowableValues;
    }

    public String getName() {
        return name;
    }

    public String getDisplayName() {
        return displayName;
    }

    public String getDescription() {
        return description;
    }

    public String getDefaultValue() {
        return defaultValue;
    }

    public Map<String,String> getAllowableValues() {
        return allowableValues;
    }

    public static final class Builder{
        private String name;
        private String displayName;
        private String description;
        private String defaultValue;
        private Map<String,String> allowableValues;

        public Builder name(String name){
            this.name = name;
            return this;
        }

        public Builder displayName(String displayName){
            this.displayName = displayName;
            return this;
        }

        public  Builder description(String description){
            this.description = description;
            return this;
        }

        public Builder defaultValue(String defaultValue){
            this.defaultValue = defaultValue;
            return this;
        }

        public Builder allowableValues(Map<String,String> allowableValues){
            this.allowableValues = allowableValues;
            return this;
        }

        public ComponentDescriptor build(){
            return new ComponentDescriptor(this);
        }
    }
}
