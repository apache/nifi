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
package org.apache.nifi.jasn1;

import com.beanit.asn1bean.ber.types.BerType;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.LoadingCache;
import org.apache.nifi.util.Tuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.lang.reflect.ParameterizedType;
import java.util.Arrays;

public class JASN1Utils {

    private static Logger logger = LoggerFactory.getLogger(JASN1Utils.class);

    /**
     * Since the same class can be read many times in case reading an array, having a cache can reduce processing time.
     * Class.getDeclaredMethod is time consuming.
     */
    private static final LoadingCache<Tuple<Class<? extends BerType>, String>, Method> getterCache = Caffeine.newBuilder()
        .maximumSize(1000)
        .build(JASN1Utils::getGetter);

    private static Method getGetter(Tuple<Class<? extends BerType>, String> methodKey) {
        Class<? extends BerType> clazz = methodKey.getKey();
        String methodName = methodKey.getValue();
        try {
            try {
                return clazz.getMethod(methodName);
            } catch (NoSuchMethodException e){
                return clazz.getMethod(methodName.replaceAll("_", ""));
            }
        } catch (ReflectiveOperationException e) {
            throw new RuntimeException("Method '" + methodName + "' not found in '" + clazz.getSimpleName() + "'", e);
        }
    }

    public static Class getSeqOfElementType(Field seqOfField) {
        final ParameterizedType seqOfGen = (ParameterizedType) seqOfField.getGenericType();
        return (Class) seqOfGen.getActualTypeArguments()[0];
    }

    public static boolean isRecordField(Field field) {
        // Filter out any static and reserved fields.
        if ((field.getModifiers() & Modifier.STATIC) == Modifier.STATIC
            || "code".equals(field.getName())) {
            return false;
        }

        return true;
    }

    public static Field getSeqOfField(Class<?> type) {
        // jASN1 generates a class having a single List field named 'seqOf' to denote 'SEQ OF X'.
        final Object[] declaredFields = Arrays.stream(type.getDeclaredFields())
            .filter(JASN1Utils::isRecordField).toArray();
        if (declaredFields.length == 1 ) {
            final Field seqOfField = (Field) declaredFields[0];
            if ("seqOf".equals(seqOfField.getName())) {
                return seqOfField;
            }
        }
        return null;
    }

    public static String toGetterMethod(String fieldName) {
        return "get" + fieldName.substring(0, 1).toUpperCase() + fieldName.substring(1);
    }

    public static Object invokeGetter(BerType model, String methodName) {
        final Object value;
        final Class<? extends BerType> type = model.getClass();
        try {
            value = getterCache.get(new Tuple<>(type, methodName)).invoke(model);
        } catch (ReflectiveOperationException e) {
            throw new RuntimeException("Failed to invoke getter method " + methodName + " of model", e);
        }
        logger.trace("get value from {} by {}={}", model, methodName, value);
        return value;
    }
}
