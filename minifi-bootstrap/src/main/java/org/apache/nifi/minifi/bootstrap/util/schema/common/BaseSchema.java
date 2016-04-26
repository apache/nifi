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

package org.apache.nifi.minifi.bootstrap.util.schema.common;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

public class BaseSchema {

    /******* Validation Issue helper methods *******/
    public List<String> validationIssues = new LinkedList<>();

    public boolean isValid() {
        return validationIssues.isEmpty();
    }

    public List<String> getValidationIssues() {
        return validationIssues;
    }

    public String getValidationIssuesAsString() {
        StringBuilder stringBuilder = new StringBuilder();
        boolean first = true;
        for (String validationIssue : validationIssues) {
            if (!first) {
                stringBuilder.append(", ");
            }
            stringBuilder.append("[");
            stringBuilder.append(validationIssue);
            stringBuilder.append("]");
            first = false;
        }
        return stringBuilder.toString();
    }

    public void addValidationIssue(String keyName, String wrapperName, String reason) {
        validationIssues.add("'" + keyName + "' in section '" + wrapperName + "' because " + reason);
    }

    public void addIssuesIfNotNull(BaseSchema baseSchema) {
        if (baseSchema != null) {
            validationIssues.addAll(baseSchema.getValidationIssues());
        }
    }

    /******* Value Access/Interpretation helper methods *******/
    public <T> T getOptionalKeyAsType(Map valueMap, String key, Class targetClass, String wrapperName, T defaultValue) {
        return getKeyAsType(valueMap, key, targetClass, wrapperName, false, defaultValue);
    }

    public <T> T getRequiredKeyAsType(Map valueMap, String key, Class targetClass, String wrapperName) {
        return getKeyAsType(valueMap, key, targetClass, wrapperName, true, null);
    }

    <T> T getKeyAsType(Map valueMap, String key, Class targetClass, String wrapperName, boolean required, T defaultValue) {
        Object value = valueMap.get(key);
        if (value == null) {
            if (defaultValue != null) {
                return defaultValue;
            } else if(required) {
                addValidationIssue(key, wrapperName, "it was not found and it is required");
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


    public <T> T getMapAsType(Map valueMap, String key, Class targetClass, String wrapperName, boolean required) {
        Object obj = valueMap.get(key);
        return interpretValueAsType(obj, key, targetClass, wrapperName, required);
    }

    public void transformListToType(List list, String simpleListType, Class targetClass, String wrapperName){
        for (int i = 0; i < list.size(); i++) {
            Object obj = interpretValueAsType(list.get(i), simpleListType + " number " + i, targetClass, wrapperName, false);
            if (obj != null) {
                list.set(i, obj);
            }
        }
    }

    private <T> T interpretValueAsType(Object obj, String key, Class targetClass, String wrapperName, boolean required) {
        if (obj == null) {
            if (required){
                addValidationIssue(key, wrapperName, "it is a required property but was not found");
            } else {
                try {
                    return (T) targetClass.newInstance();
                } catch (InstantiationException | IllegalAccessException e) {
                    addValidationIssue(key, wrapperName, "it is optional and when attempting to create it the following exception was thrown:" + e.getMessage());
                }
            }
        } else if (obj instanceof Map) {
            Constructor<?> constructor;
            try {
                constructor = targetClass.getConstructor(Map.class);
                return (T) constructor.newInstance((Map) obj);
            } catch (NoSuchMethodException | IllegalAccessException | InstantiationException | InvocationTargetException e) {
                addValidationIssue(key, wrapperName, "it is found as a map and when attempting to interpret it the following exception was thrown:" + e.getMessage());
            }
        } else {
            try {
                return (T) obj;
            } catch (ClassCastException e) {
                addValidationIssue(key, wrapperName, "it is found but could not be parsed as a map");
            }
        }
        return null;
    }
}
