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

import java.util.Collection;
import java.util.Map;

/**
 * Details about a given component. Contains configuration and current validation errors.
 */
public class ComponentDetails {

    private final String id;
    private final String name;
    private final String type;
    private final String state;
    private final String annotationData;
    private final Map<String, String> properties;
    private final Collection<String> validationErrors;

    private ComponentDetails(final Builder builder) {
        this.id = builder.id;
        this.name = builder.name;
        this.type = builder.type;
        this.state = builder.state;
        this.annotationData = builder.annotationData;
        this.properties = builder.properties;
        this.validationErrors = builder.validationErrors;
    }

    /**
     * The component id.
     * 
     * @return 
     */
    public String getId() {
        return id;
    }

    /**
     * The component name.
     * 
     * @return 
     */
    public String getName() {
        return name;
    }

    /**
     * The component type.
     * 
     * @return 
     */
    public String getType() {
        return type;
    }
    
    /**
     * The component state.
     * 
     * @return 
     */
    public String getState() {
        return state;
    }

    /**
     * The component's annotation data.
     * 
     * @return 
     */
    public String getAnnotationData() {
        return annotationData;
    }

    /**
     * Mapping of component properties.
     * 
     * @return 
     */
    public Map<String, String> getProperties() {
        return properties;
    }

    /**
     * Current validation errors for the component.
     * 
     * @return 
     */
    public Collection<String> getValidationErrors() {
        return validationErrors;
    }

    public static final class Builder {

        private String id;
        private String name;
        private String type;
        private String state;
        private String annotationData;
        private Map<String, String> properties;
        private Collection<String> validationErrors;

        public Builder id(final String id) {
            this.id = id;
            return this;
        }

        public Builder name(final String name) {
            this.name = name;
            return this;
        }
        
        public Builder type(final String type) {
            this.type = type;
            return this;
        }

        public Builder state(final String state) {
            this.state = state;
            return this;
        }

        public Builder annotationData(final String annotationData) {
            this.annotationData = annotationData;
            return this;
        }

        public Builder properties(final Map<String, String> properties) {
            this.properties = properties;
            return this;
        }

        public Builder validateErrors(final Collection<String> validationErrors) {
            this.validationErrors = validationErrors;
            return this;
        }

        public ComponentDetails build() {
            return new ComponentDetails(this);
        }
    }
}
