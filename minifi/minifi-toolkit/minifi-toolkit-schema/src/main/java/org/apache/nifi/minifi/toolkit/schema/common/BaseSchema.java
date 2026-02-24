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

package org.apache.nifi.minifi.toolkit.schema.common;

import org.apache.nifi.minifi.toolkit.schema.exception.SchemaInstantiatonException;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;

public abstract class BaseSchema implements Schema {
    public static final String IT_WAS_NOT_FOUND_AND_IT_IS_REQUIRED = "it was not found and it is required";
    public static final String EMPTY_NAME = "empty_name";

    protected final Supplier<Map<String, Object>> mapSupplier;

    public BaseSchema() {
        this(LinkedHashMap::new);
    }

    public BaseSchema(final Supplier<Map<String, Object>> mapSupplier) {
        this.mapSupplier = mapSupplier;
    }

    /******* Validation Issue helper methods *******/
    private Collection<String> validationIssues = new HashSet<>();

    @Override
    public boolean isValid() {
        return getValidationIssues().isEmpty();
    }

    @Override
    public List<String> getValidationIssues() {
        return validationIssues.stream().sorted().collect(Collectors.toList());
    }

    @Override
    public void addValidationIssue(final String issue) {
        validationIssues.add(issue);
    }

    public void addValidationIssue(final String keyName, final String wrapperName, final String reason) {
        addValidationIssue(getIssueText(keyName, wrapperName, reason));
    }

    public static String getIssueText(final String keyName, final String wrapperName, final String reason) {
        return "'" + keyName + "' in section '" + wrapperName + "' because " + reason;
    }

    public void addIssuesIfNotNull(final BaseSchema baseSchema) {
        if (baseSchema != null) {
            validationIssues.addAll(baseSchema.getValidationIssues());
        }
    }

    public void addIssuesIfNotNull(final List<? extends BaseSchema> baseSchemas) {
        if (baseSchemas != null) {
            baseSchemas.forEach(this::addIssuesIfNotNull);
        }
    }

    /******* Value Access/Interpretation helper methods *******/
    public <T> T getOptionalKeyAsType(final Map valueMap, final String key, final Class<T> targetClass, final String wrapperName, final T defaultValue) {
        return getKeyAsType(valueMap, key, targetClass, wrapperName, false, defaultValue);
    }

    public <T> T getRequiredKeyAsType(final Map valueMap, final String key, final Class<T> targetClass, final String wrapperName) {
        return getKeyAsType(valueMap, key, targetClass, wrapperName, true, null);
    }

    <T> T getKeyAsType(final Map valueMap, final String key, final Class<T> targetClass, final String wrapperName, final boolean required, final T defaultValue) {
        final Object value = valueMap.get(key);
        if (value == null || (targetClass != String.class && "".equals(value))) {
            if (defaultValue != null) {
                return defaultValue;
            } else if (required) {
                addValidationIssue(key, wrapperName, IT_WAS_NOT_FOUND_AND_IT_IS_REQUIRED);
            }
        } else {
            if (targetClass.isInstance(value)) {
                return (T) value;
            } else {
                addValidationIssue(key, wrapperName, "it is found but could not be parsed as a " + targetClass.getSimpleName());
            }
        }
        return null;
    }

    public <T> T getMapAsType(final Map valueMap, final String key, final Class<T> targetClass, final String wrapperName, final boolean required) {
        final Object obj = valueMap.get(key);
        return interpretValueAsType(obj, key, targetClass, wrapperName, required, true);
    }

    public <T> T getMapAsType(final Map valueMap, final String key, final Class targetClass, final String wrapperName, final boolean required, final boolean instantiateIfNull) {
        final Object obj = valueMap.get(key);
        return interpretValueAsType(obj, key, targetClass, wrapperName, required, instantiateIfNull);
    }

    public <InputT, OutputT> List<OutputT> convertListToType(final List<InputT> list, final String simpleListType, final Class<? extends OutputT> targetClass, final String wrapperName) {
        if (list == null) {
            return new ArrayList<>();
        }
        final List<OutputT> result = new ArrayList<>(list.size());
        for (int i = 0; i < list.size(); i++) {
            final OutputT val = interpretValueAsType(list.get(i), simpleListType + " number " + i, targetClass, wrapperName, false, false);
            if (val != null) {
                result.add(val);
            }
        }
        return result;
    }

    public <T> List<T> getOptionalKeyAsList(final Map valueMap, final String key, final Function<Map, T> conversionFunction, final String wrapperName) {
        return convertListToType(Map.class, (List<Map>) valueMap.get(key), key, conversionFunction, wrapperName, null);
    }

    public <InputT, OutputT> List<OutputT> convertListToType(final Class<InputT> inputType, final List<InputT> list, final String simpleListType, final Function<InputT, OutputT> conversionFunction,
                                                             final String wrapperName, final Supplier<OutputT> instantiator) {
        if (list == null) {
            return new ArrayList<>();
        }
        final List<OutputT> result = new ArrayList<>(list.size());
        for (int i = 0; i < list.size(); i++) {
            try {
                final OutputT val = interpretValueAsType(inputType, list.get(i), conversionFunction, instantiator);
                if (val != null) {
                    result.add(val);
                }
            } catch (final SchemaInstantiatonException e) {
                addValidationIssue(simpleListType + " number " + i, wrapperName, e.getMessage());
            }
        }
        return result;
    }

    private <InputT, OutputT> OutputT interpretValueAsType(final Class<InputT> inputType, final InputT input, final Function<InputT, OutputT> conversionFunction, final Supplier<OutputT> instantiator)
            throws SchemaInstantiatonException {
        if (input == null && instantiator != null) {
            return instantiator.get();
        }
        if (!inputType.isInstance(input)) {
            throw new SchemaInstantiatonException("was expecting object of type " + inputType + " but was " + input.getClass());
        }
        return conversionFunction.apply(input);
    }

    private <T> T interpretValueAsType(final Object obj, final String key, final Class targetClass, final String wrapperName, final boolean required, final boolean instantiateIfNull) {
        if (obj == null || (targetClass != String.class && "".equals(obj))) {
            if (required) {
                addValidationIssue(key, wrapperName, "it is a required property but was not found");
            } else {
                if (instantiateIfNull) {
                    try {
                        return (T) targetClass.getDeclaredConstructor().newInstance();
                    } catch (final InstantiationException | IllegalAccessException | NoSuchMethodException | InvocationTargetException e) {
                        addValidationIssue(key, wrapperName, "no value was given, and it is supposed to be created with default values as a default, and when attempting to create it the following " +
                                "exception was thrown:" + e.getMessage());
                    }
                }
            }
        } else if (obj instanceof Map) {
            final Constructor<?> constructor;
            try {
                constructor = targetClass.getConstructor(Map.class);
                return (T) constructor.newInstance((Map) obj);
            } catch (final NoSuchMethodException | IllegalAccessException | InstantiationException | InvocationTargetException e) {
                addValidationIssue(key, wrapperName, "it is found as a map and when attempting to interpret it the following exception was thrown:" + e.getMessage());
            }
        } else {
            try {
                return (T) obj;
            } catch (final ClassCastException e) {
                addValidationIssue(key, wrapperName, "it is found but could not be parsed as a map");
            }
        }
        return null;
    }

    public static void putIfNotNull(final Map valueMap, final String key, final WritableSchema schema) {
        if (schema != null) {
            valueMap.put(key, schema.toMap());
        }
    }

    public static void putListIfNotNull(final Map valueMap, final String key, final List<? extends WritableSchema> list) {
        if (list != null) {
            valueMap.put(key, list.stream().map(WritableSchema::toMap).collect(Collectors.toList()));
        }
    }

    public static void checkForDuplicates(final Consumer<String> duplicateMessageConsumer, final String errorMessagePrefix, final List<String> strings) {
        if (strings != null) {
            final CollectionOverlap<String> collectionOverlap = new CollectionOverlap<>(strings);
            if (!collectionOverlap.getDuplicates().isEmpty()) {
                duplicateMessageConsumer.accept(errorMessagePrefix + collectionOverlap.getDuplicates().stream().collect(Collectors.joining(", ")));
            }
        }
    }

    @Override
    public void clearValidationIssues() {
        validationIssues.clear();
    }
}
