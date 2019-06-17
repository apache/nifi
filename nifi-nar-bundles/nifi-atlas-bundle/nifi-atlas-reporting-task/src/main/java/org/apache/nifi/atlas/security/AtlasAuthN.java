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
package org.apache.nifi.atlas.security;

import org.apache.atlas.AtlasClientV2;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.ValidationContext;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.context.PropertyContext;

import java.util.Collection;
import java.util.Optional;
import java.util.Properties;

public interface AtlasAuthN {
    AtlasClientV2 createClient(final String[] baseUrls);
    Collection<ValidationResult> validate(final ValidationContext context);
    void configure(final PropertyContext context);

    /**
     * Populate required Atlas application properties.
     * This method is called when Atlas reporting task generates atlas-application.properties.
     */
    default void populateProperties(final Properties properties){}

    default Optional<ValidationResult> validateRequiredField(ValidationContext context, PropertyDescriptor prop) {
        if (!context.getProperty(prop).isSet()) {
            return Optional.of(new ValidationResult.Builder()
                    .subject(prop.getDisplayName())
                    .valid(false)
                    .explanation(String.format("required by '%s' auth.", this.getClass().getSimpleName()))
                    .build());
        }
        return Optional.empty();
    }
}
