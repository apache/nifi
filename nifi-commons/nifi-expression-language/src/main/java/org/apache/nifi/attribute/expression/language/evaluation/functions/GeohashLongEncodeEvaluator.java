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
import org.apache.nifi.attribute.expression.language.evaluation.QueryResult;
import org.apache.nifi.attribute.expression.language.evaluation.WholeNumberEvaluator;
import org.apache.nifi.attribute.expression.language.evaluation.WholeNumberQueryResult;
import org.apache.nifi.attribute.expression.language.exception.AttributeExpressionLanguageException;

import ch.hsr.geohash.GeoHash;

public class GeohashLongEncodeEvaluator extends WholeNumberEvaluator {

    private final Evaluator<Number> latitude;
    private final Evaluator<Number> longitude;
    private final Evaluator<Long> level;

    public GeohashLongEncodeEvaluator(final Evaluator<Number> latitude, final Evaluator<Number> longitude, final Evaluator<Long> level) {
        this.latitude = latitude;
        this.longitude = longitude;
        this.level = level;
    }
    @Override
    public QueryResult<Long> evaluate(final EvaluationContext evaluationContext) {
        final Number latitudeValue = latitude.evaluate(evaluationContext).getValue();
        if (latitudeValue == null) {
            return new WholeNumberQueryResult(null);
        }

        final Number longitudeValue = longitude.evaluate(evaluationContext).getValue();
        if (longitudeValue == null) {
            return new WholeNumberQueryResult(null);
        }

        final Long levelValue = level.evaluate(evaluationContext).getValue();
        if(levelValue == null) {
            return new WholeNumberQueryResult(null);
        }

        try {
            return new WholeNumberQueryResult(GeoHash.withCharacterPrecision(latitudeValue.doubleValue(), longitudeValue.doubleValue(), levelValue.intValue()).longValue());
        } catch (IllegalArgumentException e) {
            throw new AttributeExpressionLanguageException("Unable to encode lat/lon to the long format of Geohash", e);
        }
    }

    @Override
    public Evaluator<?> getSubjectEvaluator() {
        return null;
    }
}
