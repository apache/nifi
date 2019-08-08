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

import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import org.apache.commons.lang3.time.DateUtils;
import org.apache.commons.math3.linear.SingularMatrixException;
import org.apache.nifi.util.Tuple;
import org.junit.Test;

public class TestOrdinaryLeastSqauresMSAM {


    @Test
    public void testConstantPrediction(){

        Double timestamp = 1565444720000.0;
        Double inputCount = 1000.0;
        Double outputCount = 1000.0;
        Double queueCount = 50.0;

        Double[] feature0 = {timestamp - 1000,outputCount/inputCount};
        Double[] feature1 = {timestamp,outputCount/inputCount};
        Double[] feature2 = {timestamp + 1000,outputCount/inputCount};
        Double[] feature3 = {timestamp + 2000,outputCount/inputCount};

        Double[][] features = {feature0, feature1,feature2,feature3};
        Double[] labels = {queueCount,queueCount,queueCount, queueCount};

        OrdinaryLeastSquaresMSAM model = new OrdinaryLeastSquaresMSAM();
        boolean exOccurred = false;
        try {
            model.learn(Stream.of(features), Stream.of(labels));
        } catch (SingularMatrixException sme){
            exOccurred = true;
        }
        assertTrue(exOccurred);

    }

    @Test
    public void testVaryingPredictionOfVariable(){

        Double timestamp = 1565444720000.0;
        Double inputCount = 1000.0;
        Double outputCount = 50.0;
        Double queueCount = 950.0;

        Double[] feature0 = {timestamp,outputCount/inputCount};
        Double[] feature1 = {timestamp + 1000,outputCount/(inputCount + 50)};
        Double[] feature2 = {timestamp + 2000,(outputCount + 50)/(inputCount)};
        Double[] feature3 = {timestamp + 3000,(outputCount + 100)/(inputCount - 100)};

        Double[][] features = {feature0, feature1,feature2,feature3};
        Double[] labels = {queueCount,queueCount + 50, queueCount - 50, queueCount - 100};

        OrdinaryLeastSquaresMSAM model = new OrdinaryLeastSquaresMSAM();

        model.learn(Stream.of(features), Stream.of(labels));

        Tuple<Integer,Double> ratioPredictor = new Tuple<>(1,200/800.0);
        List<Tuple<Integer,Double>> predictorVars = new ArrayList<>();
        predictorVars.add(ratioPredictor);
        Double target = model.predictVariable(0,predictorVars, 750.0);
        Double rSquared = model.getRSquared();
        assert(rSquared > .90);
        Date targetDate = new Date(target.longValue());
        Date testDate = new Date(timestamp.longValue());
        assert(DateUtils.isSameDay(targetDate,testDate) && targetDate.after(testDate));

    }

    @Test
    public void testVaryingPrediction(){

        Double timestamp = 1565444720000.0;
        Double inputCount = 1000.0;
        Double outputCount = 50.0;
        Double queueCount = 950.0;

        Double[] feature0 = {timestamp,outputCount/inputCount};
        Double[] feature1 = {timestamp + 1000,outputCount/(inputCount + 50)};
        Double[] feature2 = {timestamp + 2000,(outputCount + 50)/(inputCount)};
        Double[] feature3 = {timestamp + 3000,(outputCount + 100)/(inputCount - 100)};

        Double[][] features = {feature0, feature1,feature2,feature3};
        Double[] labels = {queueCount,queueCount + 50, queueCount - 50, queueCount - 100};


        OrdinaryLeastSquaresMSAM model = new OrdinaryLeastSquaresMSAM();

        Double[] predictor = {timestamp + 5000, outputCount/inputCount};

        model.learn(Stream.of(features), Stream.of(labels));
        Double target = model.predict(predictor);
        Double rSquared = model.getRSquared();
        assert(rSquared > .90);
        assert(target >= 950);

    }

    @Test
    public void comparePredictions(){

        Double timestamp = 1565444720000.0;
        Double inputCount = 1000.0;
        Double outputCount = 50.0;
        Double queueCount = 950.0;

        Double[] feature0 = {timestamp,outputCount/inputCount};
        Double[] feature1 = {timestamp + 1000,outputCount/(inputCount + 50)};
        Double[] feature2 = {timestamp + 2000,(outputCount + 50)/(inputCount)};
        Double[] feature3 = {timestamp + 3000,(outputCount + 100)/(inputCount - 100)};

        Double[][] features = {feature0, feature1,feature2,feature3};
        Double[] labels = {queueCount,queueCount + 50, queueCount - 50, queueCount - 100};

        OrdinaryLeastSquaresMSAM ordinaryLeastSquaresMSAM = new OrdinaryLeastSquaresMSAM();
        SimpleRegressionBSAM simpleRegressionBSAM = new SimpleRegressionBSAM(false);

        ordinaryLeastSquaresMSAM.learn(Stream.of(features), Stream.of(labels));
        simpleRegressionBSAM.learn(Stream.of(features), Stream.of(labels));
        double olsR2 = ordinaryLeastSquaresMSAM.getRSquared();
        double srR2 = simpleRegressionBSAM.getRSquared();
        assert(!Double.isNaN(olsR2));
        assert(!Double.isNaN(srR2));
        Map<String,Double> olsScores = ordinaryLeastSquaresMSAM.getScores();
        Map<String,Double> srScores = simpleRegressionBSAM.getScores();
        System.out.print(olsScores.toString());
        System.out.print(srScores.toString());
        assert(olsR2 > srR2);

    }




}
