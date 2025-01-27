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
package org.apache.nifi.processors.standard.faker;

import net.datafaker.Faker;
import net.datafaker.service.files.EnFile;
import org.apache.nifi.components.AllowableValue;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.serialization.record.DataType;
import org.apache.nifi.serialization.record.RecordFieldType;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

public class FakerUtils {

    public static final String DEFAULT_DATE_PROPERTY_NAME = "DateAndTime.pastDate";
    private static final int RANDOM_DATE_DAYS = 365;
    private static final Map<String, FakerMethodHolder> datatypeFunctionMap = new LinkedHashMap<>();

    private static final List<String> PROVIDER_PACKAGES = List.of("base", "entertainment", "food", "sport", "videogame");

    // Additional Faker datatypes that don't use predetermined data files (i.e. they generate data or have non-String types)
    static final AllowableValue FT_BOOL = new AllowableValue("Boolean.bool", "Boolean - bool (true/false)", "A value of 'true' or 'false'");
    static final AllowableValue FT_FUTURE_DATE = new AllowableValue("DateAndTime.futureDate", "Date And Time - Future Date", "Generates a date up to one year in the " +
            "future from the time the processor is executed");
    static final AllowableValue FT_PAST_DATE = new AllowableValue(DEFAULT_DATE_PROPERTY_NAME, "Date And Time - Past Date", "Generates a date up to one year in the past from the time the " +
            "processor is executed");
    static final AllowableValue FT_BIRTHDAY = new AllowableValue("DateAndTime.birthday", "Date And Time - Birthday", "Generates a random birthday between 65 and 18 years ago");
    static final AllowableValue FT_NUMBER = new AllowableValue("Number.Integer", "Number - Integer", "A integer number");
    static final AllowableValue FT_SHA256 = new AllowableValue("Crypto.SHA-256", "Crypto - SHA-256", "A SHA-256 hash");
    static final AllowableValue FT_SHA512 = new AllowableValue("Crypto.SHA-512", "Crypto - SHA-512", "A SHA-512 hash");

    private static final String PACKAGE_PREFIX = "net.datafaker.providers";

    public static AllowableValue[] createFakerPropertyList() {
        final List<EnFile> fakerFiles = EnFile.getFiles().toList();
        final Map<String, Class<?>> possibleFakerTypeMap = new HashMap<>(fakerFiles.size());
        for (EnFile fakerFile : fakerFiles) {
            String className = normalizeClassName(fakerFile.getFile().substring(0, fakerFile.getFile().indexOf('.')));
            try {
                // The providers are in different sub-packages, try them all until one succeeds
                Class<?> fakerTypeClass = null;
                for (String subPackage : PROVIDER_PACKAGES) {
                    try {
                        fakerTypeClass = Class.forName(PACKAGE_PREFIX + '.' + subPackage + "." + className);
                        break;
                    } catch (ClassNotFoundException ignored) {
                        // Ignore, check the other subpackages
                    }
                }

                if (fakerTypeClass != null) {
                    possibleFakerTypeMap.put(className, fakerTypeClass);
                }
            } catch (Exception ignored) {
                // Ignore, these are the ones we want to filter out
            }
        }

        // Filter on no-arg methods that return a String, these should be the methods the user can use to generate data
        Faker faker = new Faker();
        List<AllowableValue> supportedDataTypes = new ArrayList<>();
        for (Map.Entry<String, Class<?>> entry : possibleFakerTypeMap.entrySet()) {
            List<Method> fakerMethods = Arrays.stream(entry.getValue().getDeclaredMethods()).filter((method) ->
                            Modifier.isPublic(method.getModifiers())
                                    && method.getParameterCount() == 0
                                    && method.getReturnType() == String.class)
                    .toList();
            try {
                final Object methodObject = faker.getClass().getMethod(normalizeMethodName(entry.getKey())).invoke(faker);
                for (Method method : fakerMethods) {
                    final String allowableValueName = normalizeClassName(entry.getKey()) + "." + method.getName();
                    final String allowableValueDisplayName = normalizeDisplayName(entry.getKey()) + " - " + normalizeDisplayName(method.getName());
                    datatypeFunctionMap.put(allowableValueName, new FakerMethodHolder(allowableValueName, methodObject, method));
                    supportedDataTypes.add(new AllowableValue(allowableValueName, allowableValueDisplayName, allowableValueDisplayName));
                }
            } catch (Exception ignored) {
                // Ignore, this should indicate a Faker method that we're not interested in
            }
        }

        // Add types manually for those Faker methods that generate data rather than getting it from a resource file
        supportedDataTypes.add(FT_FUTURE_DATE);
        supportedDataTypes.add(FT_PAST_DATE);
        supportedDataTypes.add(FT_BIRTHDAY);
        supportedDataTypes.add(FT_NUMBER);
        supportedDataTypes.add(FT_SHA256);
        supportedDataTypes.add(FT_SHA512);
        supportedDataTypes.sort(Comparator.comparing(AllowableValue::getDisplayName));

        return supportedDataTypes.toArray(new AllowableValue[]{});
    }

    public static Object getFakeData(String type, Faker faker) {

        // Handle Number method not discovered by programmatically getting methods from the Faker objects
        if (FT_NUMBER.getValue().equals(type)) {
            return faker.number().numberBetween(Integer.MIN_VALUE, Integer.MAX_VALUE);
        }

        // Handle DateAndTime methods not discovered by programmatically getting methods from the Faker objects
        if (FT_FUTURE_DATE.getValue().equals(type)) {
            return faker.timeAndDate().future(RANDOM_DATE_DAYS, TimeUnit.DAYS);
        }
        if (FT_PAST_DATE.getValue().equals(type)) {
            return faker.timeAndDate().past(RANDOM_DATE_DAYS, TimeUnit.DAYS);
        }
        if (FT_BIRTHDAY.getValue().equals(type)) {
            return faker.timeAndDate().birthday();
        }

        // Handle Crypto methods not discovered by programmatically getting methods from the Faker objects
        if (FT_SHA256.getValue().equals(type)) {
            return faker.hashing().sha256();
        }
        if (FT_SHA512.getValue().equals(type)) {
            return faker.hashing().sha512();
        }

        // If not a special circumstance, use the map to call the associated Faker method and return the value
        try {
            final FakerMethodHolder fakerMethodHolder = datatypeFunctionMap.get(type);
            return fakerMethodHolder.getMethod().invoke(fakerMethodHolder.getMethodObject());
        } catch (InvocationTargetException | IllegalAccessException e) {
            throw new ProcessException(type + " is not a valid value", e);
        }
    }

    // This method overrides the default String type for certain Faker datatypes for more user-friendly values
    public static DataType getDataType(final String type) {

        if (FT_FUTURE_DATE.getValue().equals(type)
                || FT_PAST_DATE.getValue().equals(type)
                || FT_BIRTHDAY.getValue().equals(type)
        ) {
            return RecordFieldType.DATE.getDataType();
        }
        if (FT_NUMBER.getValue().equals(type)) {
            return RecordFieldType.INT.getDataType();
        }
        if (FT_BOOL.getValue().equals(type)) {
            return RecordFieldType.BOOLEAN.getDataType();
        }
        return RecordFieldType.STRING.getDataType();
    }

    public static Map<String, FakerMethodHolder> getDatatypeFunctionMap() {
        return datatypeFunctionMap;
    }

    // This method identifies "segments" by splitting the given name on underscores, then capitalizes each segment and removes the underscores. Ex: 'game_of_thrones' = 'GameOfThrones'
    private static String normalizeClassName(String name) {
        String[] segments = name.split("_");

        return Arrays.stream(segments)
                .map(s -> s.substring(0, 1).toUpperCase() + s.substring(1))
                .collect(Collectors.joining());
    }

    // This method lowercases the first letter of the given name in order to match the name to a Faker method
    private static String normalizeMethodName(String name) {
        return name.substring(0, 1).toLowerCase() + name.substring(1);
    }

    // This method splits the given name on uppercase letters, ensures the first letter is capitalized, then joins the segments using a space. Ex. 'gameOfThrones' = 'Game Of Thrones'
    private static String normalizeDisplayName(String name) {
        // Split when the next letter is uppercase
        String[] upperCaseSegments = name.split("(?=\\p{Upper})");

        return Arrays.stream(upperCaseSegments).map(
                        upperCaseSegment -> upperCaseSegment.substring(0, 1).toUpperCase() + upperCaseSegment.substring(1))
                .collect(Collectors.joining(" "));
    }
}
