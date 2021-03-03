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
package org.apache.nifi.processors;

import org.apache.commons.lang.ArrayUtils;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.ValidationContext;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.util.StringUtils;

import java.util.Map;

/**
 * This class provides a validator which checks that at least one property in a provided list
 * of properties of the WURFL enrich processor is not blank.
 * If none of the properties in the list has a value, the validation fails.
 */
public class AtLeastOneNonEmptyPropertyValidator {

    public ValidationResult validate(ValidationContext validationContext, PropertyDescriptor... propertiesToValidate) {

        if (ArrayUtils.isEmpty(propertiesToValidate)){
            return new ValidationResult.Builder()
                    .subject("general")
                    .valid(false)
                    .explanation("Cannot create AtLeastOneValidator without a list of properties to validate").build();
        }

        String[] propertiesDisplayNames = new String[propertiesToValidate.length];
        for(int i = 0; i < propertiesToValidate.length; i++){
            propertiesDisplayNames[i] = propertiesToValidate[i].getDisplayName();
        }
        String propertyListForMessage = String.join(",", propertiesDisplayNames);

        Map<String,String> properties = validationContext.getAllProperties();
        for(PropertyDescriptor property: propertiesToValidate){
            String propertyValue = properties.get(property.getName());
            if (StringUtils.isNotBlank(propertyValue)){
                return new ValidationResult.Builder()
                        .subject("At least one of " + propertyListForMessage)
                        .valid(true)
                        .explanation(property.getName() + " has a non blank value ").build();
            }
        }

        // None was valid
        return new ValidationResult.Builder()
                .subject("One of the properties: " + propertyListForMessage)
                .valid(false)
                .explanation(" at least one of them must have a non blank value ")
                .build();
    }
}
