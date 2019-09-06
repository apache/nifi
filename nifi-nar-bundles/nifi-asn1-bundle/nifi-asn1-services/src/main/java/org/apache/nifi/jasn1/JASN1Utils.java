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

import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.lang.reflect.ParameterizedType;
import java.util.Arrays;

class JASN1Utils {

    static Class getSeqOfElementType(Field seqOfField) {
        final ParameterizedType seqOfGen = (ParameterizedType) seqOfField.getGenericType();
        return (Class) seqOfGen.getActualTypeArguments()[0];
    }

    static boolean isRecordField(Field field) {
        // Filter out any static and reserved fields.
        if ((field.getModifiers() & Modifier.STATIC) == Modifier.STATIC
            || "code".equals(field.getName())) {
            return false;
        }

        return true;
    }

    static Field getSeqOfField(Class<?> type) {
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

}
