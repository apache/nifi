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
package org.apache.nifi.attribute.expression.language.evaluation.functions;

import org.apache.commons.lang3.EnumUtils;
import org.apache.nifi.attribute.expression.language.EvaluationContext;
import org.apache.nifi.attribute.expression.language.evaluation.Evaluator;
import org.apache.nifi.attribute.expression.language.evaluation.QueryResult;
import org.apache.nifi.attribute.expression.language.evaluation.StringEvaluator;
import org.apache.nifi.attribute.expression.language.evaluation.StringQueryResult;
import org.apache.nifi.attribute.expression.language.exception.AttributeExpressionLanguageException;

import ch.hsr.geohash.GeoHash;


public class GeohashStringEncodeEvaluator extends StringEvaluator {

    public enum GeohashStringFormat {
        BASE32, BINARY
    }

    private final Evaluator<Number> latitude;
    private final Evaluator<Number> longitude;
    private final Evaluator<Long> level;
    private final Evaluator<String> format;

    public GeohashStringEncodeEvaluator(final Evaluator<Number> latitude, final Evaluator<Number> longitude, final Evaluator<Long> level, final Evaluator<String> format) {
        this.latitude = latitude;
        this.longitude = longitude;
        this.level = level;
        this.format = format;
    }

    @Override
    public QueryResult<String> evaluate(final EvaluationContext evaluationContext) {
        final Number latitudeValue = latitude.evaluate(evaluationContext).getValue();
        if (latitudeValue == null) {
            final Object latitudeSubjectValue = latitude.getSubjectEvaluator().evaluate(evaluationContext).getValue();
            if (latitudeSubjectValue != null && !latitudeSubjectValue.toString().isEmpty()) {
                throw new AttributeExpressionLanguageException("Unable to cast provided latitude values to Number");
            }
            return new StringQueryResult(null);
        }
        if (latitudeValue.doubleValue() < -90 || latitudeValue.doubleValue() > 90) {
            throw new AttributeExpressionLanguageException("Latitude values must be between -90 and 90.");
        }

        final Number longitudeValue = longitude.evaluate(evaluationContext).getValue();
        if (longitudeValue == null) {
            final Object longitudeSubjectValue = longitude.getSubjectEvaluator().evaluate(evaluationContext).getValue();
            if (longitudeSubjectValue != null && !longitudeSubjectValue.toString().isEmpty()) {
                throw new AttributeExpressionLanguageException("Unable to cast provided longitude values to Number");
            }
            return new StringQueryResult(null);
        }
        if (longitudeValue.doubleValue() < -180 || longitudeValue.doubleValue() > 180) {
            throw new AttributeExpressionLanguageException("Longitude values must be between -180 and 180.");
        }

        final Long levelValue = level.evaluate(evaluationContext).getValue();
        if (levelValue == null || levelValue < 1 || levelValue > 12) {
            throw new AttributeExpressionLanguageException("Level values must be between 1 and 12");
        }

        //Optional argument. If not specified, defaults to BASE_32_STRING.
        final GeohashStringFormat geohashStringFormatValue;
        if (format != null) {
            if (!EnumUtils.isValidEnum(GeohashStringFormat.class, format.evaluate(evaluationContext).getValue())) {
                throw new AttributeExpressionLanguageException("Format values must be either 'BASE32' or 'BINARY'");
            }
            geohashStringFormatValue = GeohashStringFormat.valueOf(format.evaluate(evaluationContext).getValue());
        } else {
            geohashStringFormatValue = GeohashStringFormat.BASE32;
        }

        try {
            GeoHash gh = GeoHash.withCharacterPrecision(latitudeValue.doubleValue(), longitudeValue.doubleValue(), levelValue.intValue());
            switch (geohashStringFormatValue) {
                case BINARY:
                    return new StringQueryResult(gh.toBinaryString());
                default:
                    return new StringQueryResult(gh.toBase32());
            }
        } catch (IllegalArgumentException e) {
            throw new AttributeExpressionLanguageException("Unable to encode lat/lon to the string format of Geohash", e);
        }
    }

    @Override
    public Evaluator<?> getSubjectEvaluator() {
        return null;
    }
}
