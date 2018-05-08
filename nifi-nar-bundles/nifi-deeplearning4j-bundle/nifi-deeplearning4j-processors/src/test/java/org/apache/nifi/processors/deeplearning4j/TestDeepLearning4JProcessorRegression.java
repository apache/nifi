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
package org.apache.nifi.processors.deeplearning4j;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.StringReader;
import java.util.Collections;
import java.util.List;
import java.util.Random;

import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.deeplearning4j.datasets.iterator.impl.ListDataSetIterator;
import org.deeplearning4j.nn.conf.MultiLayerConfiguration;
import org.deeplearning4j.nn.conf.NeuralNetConfiguration;
import org.deeplearning4j.nn.conf.layers.DenseLayer;
import org.deeplearning4j.nn.conf.layers.OutputLayer;
import org.deeplearning4j.nn.multilayer.MultiLayerNetwork;
import org.deeplearning4j.nn.weights.WeightInit;
import org.deeplearning4j.optimize.listeners.ScoreIterationListener;
import org.deeplearning4j.util.ModelSerializer;
import org.nd4j.linalg.activations.Activation;
import org.nd4j.linalg.api.ndarray.INDArray;
import org.nd4j.linalg.api.ops.impl.transforms.Sin;
import org.nd4j.linalg.dataset.DataSet;
import org.nd4j.linalg.dataset.api.iterator.DataSetIterator;
import org.nd4j.linalg.factory.Nd4j;
import org.nd4j.linalg.learning.config.Nesterovs;
import org.nd4j.linalg.lossfunctions.LossFunctions;
import com.google.gson.Gson;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * This test case builds a regression model based on deeplearning4j examples.
 */
public class TestDeepLearning4JProcessorRegression {

    private static File regressionModelFile;
    private static int regressionInputNumber;
    private TestRunner runner;
    protected MultiLayerNetwork model = null;
    protected Gson gson = new Gson();

    @BeforeClass
    public static void setUpModel() throws FileNotFoundException, IOException, InterruptedException {
        int nSamples = 1000;
        Random random = new Random(42);
        INDArray x = Nd4j.linspace(-Math.PI,Math.PI, nSamples ).reshape(nSamples, 1);

        INDArray sinX = Nd4j.getExecutioner().execAndReturn(new Sin(x.dup()));
        DataSet allData = new DataSet(x,sinX);
        final List<DataSet> list = allData.asList();
        Collections.shuffle(list,random);
        final DataSetIterator iterator = new ListDataSetIterator<DataSet>(list,100);
        regressionInputNumber = 1;
        long seed = 42;
        int numberOfNodes = 50;

        MultiLayerConfiguration conf = new NeuralNetConfiguration.Builder()
                .seed(seed)
                .weightInit(WeightInit.XAVIER)
                .updater(new Nesterovs(0.01, 0.9))
            .list()
            .layer(0, new DenseLayer.Builder().nIn(regressionInputNumber).nOut(numberOfNodes)
                    .activation(Activation.TANH)
                    .build())
            .layer(1, new DenseLayer.Builder().nIn(numberOfNodes).nOut(numberOfNodes)
                    .activation(Activation.TANH).build())
            .layer(1, new OutputLayer.Builder(LossFunctions.LossFunction.MSE)
                    .activation(Activation.IDENTITY)
                    .nIn(numberOfNodes).nOut(1).build())
            .pretrain(false).backprop(true).build();

        MultiLayerNetwork regressionModel = new MultiLayerNetwork(conf);
        regressionModel.init();
        regressionModel.setListeners(new ScoreIterationListener(100));
        for(int i = 0; i< 2000; i++ ) {
            regressionModel.fit(iterator);
        }
        regressionModelFile = File.createTempFile("regression-model", ".zip", new File("./"));
        regressionModelFile.deleteOnExit();

        ModelSerializer.writeModel(regressionModel, regressionModelFile ,false);
    }

    @AfterClass
    public static void deleteModel() throws FileNotFoundException, IOException, InterruptedException {
        regressionModelFile.delete();
    }

    @Before
    public void setUp() throws IOException {
        runner = TestRunners.newTestRunner(DeepLearning4JPredictor.class);
        runner.setProperty(DeepLearning4JPredictor.RECORD_DIMENSIONS,"1,1");
        runner.setProperty(DeepLearning4JPredictor.MODEL_FILE,regressionModelFile.getAbsolutePath());
        runner.assertValid();
    }

    @After
    public void teardown() {
        runner = null;
    }

    @Test
    public void testMatchWithParamsPi() {
        runner.enqueue("3.14");
        runner.run(1,true,true);
        runner.assertAllFlowFilesTransferred(DeepLearning4JPredictor.REL_SUCCESS, 1);
        List<MockFlowFile> flowFiles = runner.getFlowFilesForRelationship(DeepLearning4JPredictor.REL_SUCCESS);
        assertNull(flowFiles.get(0).getAttribute(DeepLearning4JPredictor.DEEPLEARNING4J_ERROR_MESSAGE));

        String shapeString = flowFiles.get(0).getAttribute(DeepLearning4JPredictor.DEEPLEARNING4J_OUTPUT_SHAPE);
        Integer [] shape = gson.fromJson(new StringReader(shapeString), Integer[].class);
        assertEquals("size of shape should be equal", 1, shape.length);
        assertEquals("shape should be equal", (Integer)1, shape[0]);

        Double[][] value = gson.fromJson(new StringReader(new String(flowFiles.get(0).toByteArray())),Double[][].class);
        assertEquals("size should be same", 1, value[0].length);
        assertEquals("value should be equal", 0.0d, value[0][0], 0.01d);
    }

    @Test
    public void testMatchWithParamsPiBy2() {
        runner.enqueue("1.57");
        runner.run(1,true,true);
        runner.assertAllFlowFilesTransferred(DeepLearning4JPredictor.REL_SUCCESS, 1);
        List<MockFlowFile> flowFiles = runner.getFlowFilesForRelationship(DeepLearning4JPredictor.REL_SUCCESS);
        assertNull(flowFiles.get(0).getAttribute(DeepLearning4JPredictor.DEEPLEARNING4J_ERROR_MESSAGE));

        String shapeString = flowFiles.get(0).getAttribute(DeepLearning4JPredictor.DEEPLEARNING4J_OUTPUT_SHAPE);
        Integer [] shape = gson.fromJson(new StringReader(shapeString), Integer[].class);
        assertEquals("size of shape should be equal", 1, shape.length);
        assertEquals("shape should be equal", (Integer)1, shape[0]);

        Double[][] value = gson.fromJson(new StringReader(new String(flowFiles.get(0).toByteArray())),Double[][].class);
        assertEquals("size should be same", 1, value[0].length);

        assertEquals("value should be equal", 1.0d, value[0][0], 0.01d);
    }

    @Test
    public void testMatchWith2ParamsPiAndPiBy2() {
        runner.enqueue("3.14," + System.lineSeparator() + "1.57");
        runner.setProperty(DeepLearning4JPredictor.RECORD_DIMENSIONS,"1,1");
        runner.run(1,true,true);
        runner.assertAllFlowFilesTransferred(DeepLearning4JPredictor.REL_SUCCESS, 1);
        List<MockFlowFile> flowFiles = runner.getFlowFilesForRelationship(DeepLearning4JPredictor.REL_SUCCESS);

        String shapeString = flowFiles.get(0).getAttribute(DeepLearning4JPredictor.DEEPLEARNING4J_OUTPUT_SHAPE);
        Integer [] shape = gson.fromJson(new StringReader(shapeString), Integer[].class);
        assertEquals("size of shape should be equal", 1, shape.length);
        assertEquals("shape should be equal", (Integer)1, shape[0]);

        assertNull(flowFiles.get(0).getAttribute(DeepLearning4JPredictor.DEEPLEARNING4J_ERROR_MESSAGE));
        String result = new String(flowFiles.get(0).toByteArray());
        Double[][] value = gson.fromJson(new StringReader(result),Double[][].class);
        assertEquals("size should be same", 2, value.length);

        assertEquals("value should be equal", 0.0d, value[0][0], 0.01d);
        assertEquals("value should be equal", 1.0d, value[1][0], 0.01d);
    }
}
