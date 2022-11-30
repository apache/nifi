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

package org.apache.nifi.minifi.c2.command;

import static org.apache.nifi.minifi.c2.command.UpdatePropertiesPropertyProvider.AVAILABLE_PROPERTIES;
import static org.apache.nifi.minifi.commons.api.MiNiFiConstants.BOOTSTRAP_UPDATED_FILE_NAME;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.components.Validator;
import org.apache.nifi.processor.util.StandardValidators;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PropertiesPersister {

    private static final Logger LOGGER = LoggerFactory.getLogger(PropertiesPersister.class);
    private static final String VALID = "VALID";
    private static final String EQUALS_SIGN = "=";
    private static final String HASHMARK_SIGN = "#";

    private final UpdatePropertiesPropertyProvider updatePropertiesPropertyProvider;
    private final AgentPropertyValidationContext validationContext;
    private final File bootstrapFile;
    private final File bootstrapNewFile;

    public PropertiesPersister(UpdatePropertiesPropertyProvider updatePropertiesPropertyProvider, String bootstrapConfigFileLocation) {
        this.updatePropertiesPropertyProvider = updatePropertiesPropertyProvider;
        this.validationContext = new AgentPropertyValidationContext();
        this.bootstrapFile = new File(bootstrapConfigFileLocation);
        this.bootstrapNewFile = new File(bootstrapFile.getParentFile() + "/" + BOOTSTRAP_UPDATED_FILE_NAME);
    }

    public Boolean persistProperties(Map<String, String> propertiesToUpdate) {
        int propertyCountToUpdate = validateProperties(propertiesToUpdate);
        if (propertyCountToUpdate == 0) {
            return false;
        }
        Set<String> propertiesToUpdateKeys = new HashSet<>(propertiesToUpdate.keySet());

        Set<String> updatedProperties = new HashSet<>();
        try (BufferedReader reader = new BufferedReader(new FileReader(bootstrapFile));
            BufferedWriter bufferedWriter = new BufferedWriter(new FileWriter(bootstrapNewFile, false))) {
            String line;
            while ((line = reader.readLine()) != null) {
                for (String key : propertiesToUpdateKeys) {
                    String prefix = key + EQUALS_SIGN;
                    if (line.startsWith(prefix) || line.startsWith(HASHMARK_SIGN + prefix)) {
                        line = prefix + propertiesToUpdate.get(key);
                        updatedProperties.add(key);
                    }
                }
                bufferedWriter.write(line + System.lineSeparator());
            }

            // add new properties which has no values before
            propertiesToUpdateKeys.removeAll(updatedProperties);
            for (String key : propertiesToUpdateKeys) {
                bufferedWriter.write(key + EQUALS_SIGN + propertiesToUpdate.get(key) + System.lineSeparator());
            }
        } catch (FileNotFoundException e) {
            throw new RuntimeException(e);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }

        return true;
    }

    private int validateProperties(Map<String, String> propertiesToUpdate) {
        Set<UpdatableProperty> updatableProperties = (Set<UpdatableProperty>) updatePropertiesPropertyProvider.getProperties().get(AVAILABLE_PROPERTIES);
        Map<String, UpdatableProperty> updatablePropertyMap = updatableProperties.stream().collect(Collectors.toMap(UpdatableProperty::getPropertyName, Function.identity()));
        int propertyCountToUpdate = 0;
        List<String> validationErrors = new ArrayList<>();
        for (Map.Entry<String, String> entry : propertiesToUpdate.entrySet()) {
            UpdatableProperty updatableProperty = updatablePropertyMap.get(entry.getKey());
            if (updatableProperty == null) {
                validationErrors.add(String.format("You can not update the {} property through C2 protocol", entry.getKey()));
                continue;
            }
            if (!Objects.equals(updatableProperty.getPropertyValue(), entry.getValue())) {
                if (!getValidator(updatableProperty.getValidator())
                    .map(validator -> validator.validate(entry.getKey(), entry.getValue(), validationContext))
                    .map(ValidationResult::isValid)
                    .orElse(true)) {
                    validationErrors.add(String.format("Invalid value for %s", entry.getKey()));
                    continue;
                }
                propertyCountToUpdate++;
            }
        }
        if (!validationErrors.isEmpty()) {
            throw new IllegalArgumentException("The following validation errors happened during property update:\\n" + String.join("\\n", validationErrors));
        }
        return propertyCountToUpdate;
    }

    private Optional<Validator> getValidator(String validatorName) {
        try {
            Field validatorField = StandardValidators.class.getField(validatorName);
            return Optional.of((Validator) validatorField.get(null));
        } catch (NoSuchFieldException e) {
            if (!VALID.equals(validatorName)) {
                LOGGER.warn("No validator present: {}", validatorName);
            }
        } catch (IllegalAccessException e) {
            LOGGER.error("Illegal access of {}", validatorName);
        }
        return Optional.empty();
    }
}
