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

import org.apache.nifi.attribute.expression.language.EvaluationContext;
import org.apache.nifi.attribute.expression.language.evaluation.Evaluator;
import org.apache.nifi.attribute.expression.language.evaluation.NumberEvaluator;
import org.apache.nifi.attribute.expression.language.evaluation.NumberQueryResult;
import org.apache.nifi.attribute.expression.language.evaluation.QueryResult;
import org.apache.nifi.attribute.expression.language.exception.AttributeExpressionLanguageException;

import ch.hsr.geohash.GeoHash;
import ch.hsr.geohash.WGS84Point;

public abstract class GeohashDecodeBaseEvaluator extends NumberEvaluator {

    public enum GeohashFormat {
        BASE_32_STRING, BINARY_STRING, LONG
    }
    public enum GeoCoord {
        LATITUDE, LONGITUDE
    }
    private final Evaluator<String> subject;
    private final Evaluator<String> format;

    public GeohashDecodeBaseEvaluator(final Evaluator<String> subject, final Evaluator<String> format) {
        this.subject = subject;
        this.format = format;
    }

    public QueryResult<Number> geohashDecodeEvaluate(final EvaluationContext evaluationContext, final GeoCoord geoCoord) {
        //Optional argument. If not specified, defaults to BASE_32_STRING.
        final GeohashFormat geohashFormatValue;
        if(format != null) {
            geohashFormatValue = GeohashFormat.valueOf(format.evaluate(evaluationContext).getValue());
        }else {
            geohashFormatValue = GeohashFormat.BASE_32_STRING;

        }

        final Long geohashLongValue = geohashFormatValue == GeohashFormat.LONG ? Long.valueOf(subject.evaluate(evaluationContext).getValue()) : null;
        final String geohashStringValue = (geohashFormatValue == GeohashFormat.BASE_32_STRING
                || geohashFormatValue == GeohashFormat.BINARY_STRING) ? subject.evaluate(evaluationContext).getValue() : null;

        if (geohashLongValue == null && geohashStringValue == null) {
            return new NumberQueryResult(null);
        }

        try {
            WGS84Point boundingBoxCenter;
            switch (geohashFormatValue) {
                case LONG:
                    String binaryString = Long.toBinaryString(geohashLongValue);
                    boundingBoxCenter = GeoHash.fromBinaryString(binaryString).getBoundingBoxCenter();
                    break;
                case BINARY_STRING:
                    boundingBoxCenter = GeoHash.fromBinaryString(geohashStringValue).getBoundingBoxCenter();
                    break;
                default:
                    boundingBoxCenter = GeoHash.fromGeohashString(geohashStringValue).getBoundingBoxCenter();
            }
            if(geoCoord == GeoCoord.LATITUDE) {
                return new NumberQueryResult(boundingBoxCenter.getLatitude());
            }else {
                return new NumberQueryResult(boundingBoxCenter.getLongitude());
            }

        } catch (IllegalArgumentException e) {
            throw new AttributeExpressionLanguageException("Unable to decode Geohash", e);
        }
    }

    @Override
    public Evaluator<String> getSubjectEvaluator() {
        return subject;
    }
}
