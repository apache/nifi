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

package org.apache.nifi.attribute.expression.language.evaluation.util;

import java.util.Date;

import org.apache.nifi.attribute.expression.language.evaluation.Evaluator;
import org.apache.nifi.expression.AttributeExpression.ResultType;

public class DatatypeCast {

    /**
     * Converts java class into compatible ResultType of EL
     * @param clazz to be converted
     * @return ResultType compatible with given java class.
     */
    public static ResultType java2ResultType(Class<?> clazz) {
        if (clazz == null) {
            return null;
        }

        if (String.class.isAssignableFrom(clazz)) {
            return ResultType.STRING;
        }
        if (Boolean.class.isAssignableFrom(clazz)) {
            return ResultType.BOOLEAN;
        }
        if (Integer.class.isAssignableFrom(clazz)) {
            return ResultType.WHOLE_NUMBER;
        }
        if (Long.class.isAssignableFrom(clazz)) {
            return ResultType.WHOLE_NUMBER;
        }
        if (Short.class.isAssignableFrom(clazz)) {
            return ResultType.WHOLE_NUMBER;
        }
        if (Float.class.isAssignableFrom(clazz)) {
            return ResultType.DECIMAL;
        }
        if (Double.class.isAssignableFrom(clazz)) {
            return ResultType.DECIMAL;
        }
        if (Number.class.isAssignableFrom(clazz)) {
            return ResultType.NUMBER; //default for the rest of the numbers
        }
        if (Date.class.isAssignableFrom(clazz)) {
            return ResultType.DATE;
        }

        //default type: cast to string all the way!
        return ResultType.STRING;
    }

    /**
     * Converts java class of given object into compatible ResultType of EL
     * @param obj - any sort of object, which class should be converted
     * @return ResultType compatible with given object's java class.
     */
    public static ResultType java2ResultType(Object obj) {
        return obj == null ? null : java2ResultType(obj.getClass());
    }

    /**
     * Converts EL ResultType, used for Evaluation of expression, into Java compatible class
     * @param resultType - given type of an EL expression/value
     * @return Java class matching given ResultType
     */
    public static Class<?> resultType2Java(ResultType resultType) {
        if (resultType == null) {
            return null;
        }

        switch(resultType) {
        case BOOLEAN: return Boolean.class;
        case DATE: return Date.class;
        case DECIMAL: return Double.class;
        case NUMBER: return Long.class;
        case STRING: return String.class;
        case WHOLE_NUMBER: return Long.class;
        default: {};
        }

        return null;
    }

    /**
     * Converts EL ResultType of given evaluator, used for Evaluation of expression, into Java compatible class
     * @param evaluator - evaluator of an EL expression/value
     * @return Java class matching given ResultType
     */
    public static Class<?> resultType2Java(Evaluator<?> evaluator) {
        return evaluator == null ? null : resultType2Java(evaluator.getResultType());
    }
}
