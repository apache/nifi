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
package org.apache.nifi.controller.status.analytics.models;

import static org.junit.Assert.assertNotNull;

import java.util.stream.Stream;
import org.junit.Test;

public class TestSimpleRegression {

    @Test
    public void testConstantPrediction(){

        Double timestamp = 1565444720000.0;
        Double queueCount = 50.0;

        Double[] feature0 = {timestamp - 1000};
        Double[] feature1 = {timestamp};
        Double[] feature2 = {timestamp + 1000};
        Double[] feature3 = {timestamp + 2000};

        Double[][] features = {feature0, feature1,feature2,feature3};
        Double[] labels = {queueCount,queueCount,queueCount, queueCount};

        SimpleRegression model = new SimpleRegression(false);

        model.learn(Stream.of(features), Stream.of(labels));

        Double[] predictor = {timestamp + 5000};
        Double target = model.predict(predictor);
        assertNotNull(target);
        assert(target  == 50);

    }

    @Test
    public void testVaryingPredictX(){

        Double timestamp = 1565444720000.0;
        Double queueCount = 950.0;

        Double[] feature0 = {timestamp};
        Double[] feature1 = {timestamp + 1000};
        Double[] feature2 = {timestamp + 2000};
        Double[] feature3 = {timestamp + 3000 };

        Double[][] features = {feature0, feature1,feature2,feature3};
        Double[] labels = {queueCount,queueCount + 50, queueCount - 50, queueCount - 100};

        SimpleRegression model = new SimpleRegression(false);

        model.learn(Stream.of(features), Stream.of(labels));

        Double target = model.predictX(1000.0);
        Double minTimeMillis = 1565343920000.0;
        Double maxTimeMillis = 1565516720000.0;
        assert(target >= minTimeMillis && target <= maxTimeMillis);

    }

    @Test
    public void testVaryingPredictY(){

        Double timestamp = 1565444720000.0;
        Double queueCount = 950.0;

        Double[] feature0 = {timestamp};
        Double[] feature1 = {timestamp + 1000};
        Double[] feature2 = {timestamp + 2000};
        Double[] feature3 = {timestamp + 3000};

        Double[][] features = {feature0, feature1,feature2,feature3};
        Double[] labels = {queueCount,queueCount + 50, queueCount - 50, queueCount - 100};

        SimpleRegression model = new SimpleRegression(false);

        Double[] predictor = {timestamp + 5000};

        model.learn(Stream.of(features), Stream.of(labels));
        Double target = model.predict(predictor);
        Double rSquared = model.getScores().get("rSquared");
        Double minCount = -1265.0;
        Double maxCount = 3235.0;
        assert(rSquared > .60);
        assert(target >= minCount && target <= maxCount);
    }
}
