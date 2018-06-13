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
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.StringReader;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.datavec.api.records.reader.RecordReader;
import org.datavec.api.records.reader.impl.csv.CSVRecordReader;
import org.datavec.api.split.FileSplit;
import org.datavec.api.util.ClassPathResource;
import org.deeplearning4j.datasets.datavec.RecordReaderDataSetIterator;
import org.deeplearning4j.nn.conf.MultiLayerConfiguration;
import org.deeplearning4j.nn.conf.NeuralNetConfiguration;
import org.deeplearning4j.nn.conf.layers.DenseLayer;
import org.deeplearning4j.nn.conf.layers.OutputLayer;
import org.deeplearning4j.nn.multilayer.MultiLayerNetwork;
import org.deeplearning4j.nn.weights.WeightInit;
import org.deeplearning4j.optimize.listeners.ScoreIterationListener;
import org.deeplearning4j.util.ModelSerializer;
import org.nd4j.linalg.activations.Activation;
import org.nd4j.linalg.dataset.DataSet;
import org.nd4j.linalg.dataset.SplitTestAndTrain;
import org.nd4j.linalg.dataset.api.iterator.DataSetIterator;
import org.nd4j.linalg.dataset.api.preprocessor.DataNormalization;
import org.nd4j.linalg.dataset.api.preprocessor.NormalizerStandardize;
import org.nd4j.linalg.learning.config.Sgd;
import org.nd4j.linalg.lossfunctions.LossFunctions;
import com.google.gson.Gson;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * This test case builds a classification model based on deeplearning4j examples.
 */
public class TestDeepLearning4JMultiLayerPredictorClassification {

    private static File classificationModelFile;
    private static int classficationInputNumber;
    private static int classificationOutputNumber;
    private TestRunner runner;
    protected MultiLayerNetwork model = null;
    protected Gson gson = new Gson();
    protected static final String CORRELATION_ATTRIBUTE = "ids";

    @BeforeClass
    public static void setUpModel() throws FileNotFoundException, IOException, InterruptedException {
        int numLinesToSkip = 0;
        char delimiter = ',';
        RecordReader recordReader = new CSVRecordReader(numLinesToSkip,delimiter);
        recordReader.initialize(new FileSplit(new ClassPathResource("classification_test.txt").getFile()));
        int labelIndex = 4;
        int numClasses = 4;
        int batchSize = 100;
        DataSetIterator iterator = new RecordReaderDataSetIterator(recordReader,batchSize,labelIndex,numClasses);
        DataSet allData = iterator.next();
        allData.shuffle();

        SplitTestAndTrain testAndTrain = allData.splitTestAndTrain(0.70);
        DataSet trainingData = testAndTrain.getTrain();
        DataSet testData = testAndTrain.getTest();

        DataNormalization normalizer = new NormalizerStandardize();
        normalizer.fit(trainingData);
        normalizer.transform(trainingData);
        normalizer.transform(testData);
        classficationInputNumber = 4;
        classificationOutputNumber = 4;
        long seed = 42;

        MultiLayerConfiguration conf = new NeuralNetConfiguration.Builder()
            .seed(seed)
            .weightInit(WeightInit.RELU)
            .activation(Activation.RELU)
            .updater(new Sgd(0.1))
            .l2(1e-3)
            .list()
            .layer(0, new DenseLayer.Builder().nIn(classficationInputNumber).nOut(5).build())
            .layer(1, new DenseLayer.Builder().nIn(5).nOut(5).build())
            .layer(2, new OutputLayer.Builder(LossFunctions.LossFunction.NEGATIVELOGLIKELIHOOD)
                .activation(Activation.SOFTMAX)
                .nIn(5).nOut(classificationOutputNumber).build())
            .backprop(true).pretrain(false)
            .build();

        MultiLayerNetwork classificationModel = new MultiLayerNetwork(conf);
        classificationModel.init();
        classificationModel.setListeners(new ScoreIterationListener(100));
        for(int i=0; i< 1000; i++ ) {
            classificationModel.fit(trainingData);
        }
        classificationModelFile = File.createTempFile("classification-model", ".zip", new File("./"));
        classificationModelFile.deleteOnExit();
        // save the model
        ModelSerializer.writeModel(classificationModel, classificationModelFile ,false);
    }

    @AfterClass
    public static void deleteModel() throws FileNotFoundException, IOException, InterruptedException {
        classificationModelFile.delete();
    }

    @Before
    public void setUp() throws IOException {
        runner = TestRunners.newTestRunner(DeepLearning4JMultiLayerPredictor.class);
        runner.setProperty(DeepLearning4JMultiLayerPredictor.RECORD_DIMENSIONS,"1,4");
        runner.setProperty(DeepLearning4JMultiLayerPredictor.MODEL_FILE,classificationModelFile.getAbsolutePath());
        runner.assertValid();
    }

    @After
    public void teardown() {
        runner = null;
    }

    @Test
    public void testMatchWith4ParamsClass1() {
        runner.enqueue("2.0,.5,.4,0.2");
        runner.run(1,true,true);

        runner.assertAllFlowFilesTransferred(DeepLearning4JMultiLayerPredictor.REL_SUCCESS, 1);
        List<MockFlowFile> flowFiles = runner.getFlowFilesForRelationship(DeepLearning4JMultiLayerPredictor.REL_SUCCESS);
        assertNull(flowFiles.get(0).getAttribute(DeepLearning4JMultiLayerPredictor.DEEPLEARNING4J_ERROR_MESSAGE));
        String shapeString = flowFiles.get(0).getAttribute(DeepLearning4JMultiLayerPredictor.DEEPLEARNING4J_OUTPUT_SHAPE);

        Integer [] shape = gson.fromJson(new StringReader(shapeString), Integer[].class);
        assertEquals("size of shape should be equal", 1, shape.length);
        assertEquals("shape should be equal", (Integer)4, shape[0]);
        String result = new String(flowFiles.get(0).toByteArray());

        Double[][] value = gson.fromJson(new StringReader(result),Double[][].class);
        assertEquals("size should be same", classificationOutputNumber, value[0].length);
        assertTrue("Should be first class",value[0][0] > value[0][1]);
        assertTrue("Should be first class",value[0][0] > value[0][2]);
        assertTrue("Should be first class",value[0][0] > value[0][3]);
    }

    @Test
    public void testMatchWith4ParamsClass1WithId() {
        Map<String,String> properties = new HashMap<String,String>();
        properties.put(CORRELATION_ATTRIBUTE, "1234");
        runner.enqueue("2.0,.5,.4,0.2", properties);
        runner.run(1,true,true);
        runner.assertAllFlowFilesTransferred(DeepLearning4JMultiLayerPredictor.REL_SUCCESS, 1);

        List<MockFlowFile> flowFiles = runner.getFlowFilesForRelationship(DeepLearning4JMultiLayerPredictor.REL_SUCCESS);
        String propertyValue =  flowFiles.get(0).getAttribute(CORRELATION_ATTRIBUTE);
        assertEquals("Property values should be same", "1234", propertyValue);
        assertNull(flowFiles.get(0).getAttribute(DeepLearning4JMultiLayerPredictor.DEEPLEARNING4J_ERROR_MESSAGE));
        String shapeString = flowFiles.get(0).getAttribute(DeepLearning4JMultiLayerPredictor.DEEPLEARNING4J_OUTPUT_SHAPE);

        Integer [] shape = gson.fromJson(new StringReader(shapeString), Integer[].class);
        assertEquals("size of shape should be equal", 1, shape.length);
        assertEquals("shape should be equal", (Integer)4, shape[0]);
        String result = new String(flowFiles.get(0).toByteArray());
        Double[][] value = gson.fromJson(new StringReader(result),Double[][].class);
        assertEquals("size should be same", classificationOutputNumber, value[0].length);
        assertTrue("Should be first class",value[0][0] > value[0][1]);
        assertTrue("Should be first class",value[0][0] > value[0][2]);
        assertTrue("Should be first class",value[0][0] > value[0][3]);
    }

    @Test
    public void testMatchWith2Rows4ColumnsParamsClass11() {
        runner.enqueue("2.0,.5,.4,0.2" + System.lineSeparator() + "3.0,.2,.1,.2");
        runner.setProperty(DeepLearning4JMultiLayerPredictor.RECORD_DIMENSIONS,"1,4");
        runner.run(1,true,true);
        runner.assertAllFlowFilesTransferred(DeepLearning4JMultiLayerPredictor.REL_SUCCESS, 1);

        List<MockFlowFile> flowFiles = runner.getFlowFilesForRelationship(DeepLearning4JMultiLayerPredictor.REL_SUCCESS);
        String shapeString = flowFiles.get(0).getAttribute(DeepLearning4JMultiLayerPredictor.DEEPLEARNING4J_OUTPUT_SHAPE);
        Integer [] shape = gson.fromJson(new StringReader(shapeString), Integer[].class);
        assertEquals("size of shape should be equal", 1, shape.length);
        assertEquals("shape should be equal", (Integer)4, shape[0]);
        assertNull(flowFiles.get(0).getAttribute(DeepLearning4JMultiLayerPredictor.DEEPLEARNING4J_ERROR_MESSAGE));
        String result = new String(flowFiles.get(0).toByteArray());

        Double[][] value = gson.fromJson(new StringReader(result),Double[][].class);
        assertEquals("size should be same", 2, value.length);
        assertTrue("Should be first class",value[0][0] > value[0][1]);
        assertTrue("Should be first class",value[0][0] > value[0][2]);
        assertTrue("Should be first class",value[0][0] > value[0][3]);
        assertTrue("Should be first class",value[1][0] > value[1][1]);
        assertTrue("Should be first class",value[1][0] > value[1][2]);
        assertTrue("Should be first class",value[1][0] > value[1][3]);
    }

    @Test
    public void testMatchWith2Rows4ColumnsParamsClass11WithIds() {
        Map<String,String> properties = new HashMap<String,String>();
        properties.put(CORRELATION_ATTRIBUTE, "1234,3456");
        runner.enqueue("2.0,.5,.4,0.2" + System.lineSeparator() + "3.0,.2,.1,.2", properties);
        runner.setProperty(DeepLearning4JMultiLayerPredictor.RECORD_DIMENSIONS,"1,4");
        runner.run(1,true,true);
        runner.assertAllFlowFilesTransferred(DeepLearning4JMultiLayerPredictor.REL_SUCCESS, 1);

        List<MockFlowFile> flowFiles = runner.getFlowFilesForRelationship(DeepLearning4JMultiLayerPredictor.REL_SUCCESS);
        String propertyValue =  flowFiles.get(0).getAttribute(CORRELATION_ATTRIBUTE);
        assertEquals("Property values should be same", "1234,3456", propertyValue);
        String shapeString = flowFiles.get(0).getAttribute(DeepLearning4JMultiLayerPredictor.DEEPLEARNING4J_OUTPUT_SHAPE);
        Integer [] shape = gson.fromJson(new StringReader(shapeString), Integer[].class);
        assertEquals("size of shape should be equal", 1, shape.length);
        assertEquals("shape should be equal", (Integer)4, shape[0]);
        assertNull(flowFiles.get(0).getAttribute(DeepLearning4JMultiLayerPredictor.DEEPLEARNING4J_ERROR_MESSAGE));

        String result = new String(flowFiles.get(0).toByteArray());
        Double[][] value = gson.fromJson(new StringReader(result),Double[][].class);
        assertEquals("size should be same", 2, value.length);
        assertTrue("Should be first class",value[0][0] > value[0][1]);
        assertTrue("Should be first class",value[0][0] > value[0][2]);
        assertTrue("Should be first class",value[0][0] > value[0][3]);
        assertTrue("Should be first class",value[1][0] > value[1][1]);
        assertTrue("Should be first class",value[1][0] > value[1][2]);
        assertTrue("Should be first class",value[1][0] > value[1][3]);
    }

    @Test
    public void testMatchWith4Rows4ColumnsParamsClass1123() {
        runner.enqueue("2.0,.5,.4,0.2" + System.lineSeparator() + "3.0,.2,.1,.2" + System.lineSeparator()  + ".2,2.0,.4,0.2" + System.lineSeparator() + ".2,.2,3.4,.2");
        runner.setProperty(DeepLearning4JMultiLayerPredictor.RECORD_DIMENSIONS,"1,4");
        runner.run(1,true,true);
        runner.assertAllFlowFilesTransferred(DeepLearning4JMultiLayerPredictor.REL_SUCCESS, 1);

        List<MockFlowFile> flowFiles = runner.getFlowFilesForRelationship(DeepLearning4JMultiLayerPredictor.REL_SUCCESS);
        String shapeString = flowFiles.get(0).getAttribute(DeepLearning4JMultiLayerPredictor.DEEPLEARNING4J_OUTPUT_SHAPE);

        Integer [] shape = gson.fromJson(new StringReader(shapeString), Integer[].class);
        assertEquals("size of shape should be equal", 1, shape.length);
        assertEquals("shape should be equal", (Integer)4, shape[0]);
        assertNull(flowFiles.get(0).getAttribute(DeepLearning4JMultiLayerPredictor.DEEPLEARNING4J_ERROR_MESSAGE));
        String result = new String(flowFiles.get(0).toByteArray());

        Double[][] value = gson.fromJson(new StringReader(result),Double[][].class);
        assertEquals("size should be same", 4, value.length);
        assertTrue("Should be first class",value[0][0] > value[0][1]);
        assertTrue("Should be first class",value[0][0] > value[0][2]);
        assertTrue("Should be first class",value[0][0] > value[0][3]);
        assertTrue("Should be first class",value[1][0] > value[1][1]);
        assertTrue("Should be first class",value[1][0] > value[1][2]);
        assertTrue("Should be first class",value[1][0] > value[1][3]);
        assertTrue("Should be first class",value[2][1] > value[2][0]);
        assertTrue("Should be first class",value[2][1] > value[2][2]);
        assertTrue("Should be first class",value[2][1] > value[2][3]);
        assertTrue("Should be first class",value[3][2] > value[3][0]);
        assertTrue("Should be first class",value[3][2] > value[3][1]);
        assertTrue("Should be first class",value[3][2] > value[1][3]);
    }

    @Test
    public void testMatchWith4ParamsClass1ModelFileEL() {
        runner.setVariable("modelPath", classificationModelFile.getAbsolutePath());
        runner.setProperty(DeepLearning4JMultiLayerPredictor.MODEL_FILE,"${modelPath}");

        runner.enqueue("2.0,.5,.4,0.2");
        runner.run(1,true,true);
        runner.assertAllFlowFilesTransferred(DeepLearning4JMultiLayerPredictor.REL_SUCCESS, 1);

        List<MockFlowFile> flowFiles = runner.getFlowFilesForRelationship(DeepLearning4JMultiLayerPredictor.REL_SUCCESS);
        assertNull(flowFiles.get(0).getAttribute(DeepLearning4JMultiLayerPredictor.DEEPLEARNING4J_ERROR_MESSAGE));
        String shapeString = flowFiles.get(0).getAttribute(DeepLearning4JMultiLayerPredictor.DEEPLEARNING4J_OUTPUT_SHAPE);

        Integer [] shape = gson.fromJson(new StringReader(shapeString), Integer[].class);
        assertEquals("size of shape should be equal", 1, shape.length);
        assertEquals("shape should be equal", (Integer)4, shape[0]);

        Double[][] value = gson.fromJson(new StringReader(new String(flowFiles.get(0).toByteArray())),Double[][].class);
        assertEquals("size should be same", classificationOutputNumber, value[0].length);
        assertTrue("Should be first class",value[0][0] > value[0][1]);
        assertTrue("Should be first class",value[0][0] > value[0][2]);
        assertTrue("Should be first class",value[0][0] > value[0][3]);
    }

    @Test
    public void testMatchWith4ParamsClass1DimensionEL() {
        Map<String,String> map = new HashMap<String,String>();
        map.put("dimension", "1,4");

        runner.enqueue("2.0,.5,.4,0.2",map);
        runner.setProperty(DeepLearning4JMultiLayerPredictor.RECORD_DIMENSIONS,"${dimension}");
        runner.run(1,true,true);
        runner.assertAllFlowFilesTransferred(DeepLearning4JMultiLayerPredictor.REL_SUCCESS, 1);

        List<MockFlowFile> flowFiles = runner.getFlowFilesForRelationship(DeepLearning4JMultiLayerPredictor.REL_SUCCESS);
        assertNull(flowFiles.get(0).getAttribute(DeepLearning4JMultiLayerPredictor.DEEPLEARNING4J_ERROR_MESSAGE));
        String shapeString = flowFiles.get(0).getAttribute(DeepLearning4JMultiLayerPredictor.DEEPLEARNING4J_OUTPUT_SHAPE);

        Integer [] shape = gson.fromJson(new StringReader(shapeString), Integer[].class);
        assertEquals("size of shape should be equal", 1, shape.length);
        assertEquals("shape should be equal", (Integer)4, shape[0]);

        Double[][] value = gson.fromJson(new StringReader(new String(flowFiles.get(0).toByteArray())),Double[][].class);
        assertEquals("size should be same", classificationOutputNumber, value[0].length);
        assertTrue("Should be first class",value[0][0] > value[0][1]);
        assertTrue("Should be first class",value[0][0] > value[0][2]);
        assertTrue("Should be first class",value[0][0] > value[0][3]);
    }

    @Test
    public void testMatchWith4ParamsClass1SeparatorEL() {
        Map<String,String> map = new HashMap<String,String>();
        map.put("separator", ";");

        runner.enqueue("2.0;0.5;0.4;0.2",map);
        runner.setProperty(DeepLearning4JMultiLayerPredictor.FIELD_SEPARATOR,"${separator}");
        runner.setProperty(DeepLearning4JMultiLayerPredictor.RECORD_DIMENSIONS,"1;4");
        runner.run(1,true,true);
        runner.assertAllFlowFilesTransferred(DeepLearning4JMultiLayerPredictor.REL_SUCCESS, 1);

        List<MockFlowFile> flowFiles = runner.getFlowFilesForRelationship(DeepLearning4JMultiLayerPredictor.REL_SUCCESS);
        assertNull(flowFiles.get(0).getAttribute(DeepLearning4JMultiLayerPredictor.DEEPLEARNING4J_ERROR_MESSAGE));
        String shapeString = flowFiles.get(0).getAttribute(DeepLearning4JMultiLayerPredictor.DEEPLEARNING4J_OUTPUT_SHAPE);

        Integer [] shape = gson.fromJson(new StringReader(shapeString), Integer[].class);
        assertEquals("size of shape should be equal", 1, shape.length);
        assertEquals("shape should be equal", (Integer)4, shape[0]);

        Double[][] value = gson.fromJson(new StringReader(new String(flowFiles.get(0).toByteArray())),Double[][].class);
        assertEquals("size should be same", classificationOutputNumber, value[0].length);
        assertTrue("Should be first class",value[0][0] > value[0][1]);
        assertTrue("Should be first class",value[0][0] > value[0][2]);
        assertTrue("Should be first class",value[0][0] > value[0][3]);
    }

    @Test
    public void testMatchWith4ParamsOneNonNumber() {
        runner.enqueue("5.1,3.5,1.4,abcd");
        runner.run(1,true,true);
        runner.assertAllFlowFilesTransferred(DeepLearning4JMultiLayerPredictor.REL_FAILURE, 1);
        List<MockFlowFile> flowFiles = runner.getFlowFilesForRelationship(DeepLearning4JMultiLayerPredictor.REL_FAILURE);
        assertNotNull(flowFiles.get(0).getAttribute(DeepLearning4JMultiLayerPredictor.DEEPLEARNING4J_ERROR_MESSAGE));
    }

    @Test
    public void testNoDimensionsInvalid() {
        runner = TestRunners.newTestRunner(DeepLearning4JMultiLayerPredictor.class);
        runner.setProperty(DeepLearning4JMultiLayerPredictor.MODEL_FILE,classificationModelFile.getAbsolutePath());
        runner.assertNotValid();
    }

    @Test
    public void testNoModelInvalid() {
        runner = TestRunners.newTestRunner(DeepLearning4JMultiLayerPredictor.class);
        runner.setProperty(DeepLearning4JMultiLayerPredictor.RECORD_DIMENSIONS,"1,4");
        runner.assertNotValid();
    }

    @Test
    public void testMatchWith3ParamsBad() {
        runner.enqueue("5.1,3.5,1.4");
        runner.run(1,true,true);
        runner.assertAllFlowFilesTransferred(DeepLearning4JMultiLayerPredictor.REL_FAILURE, 1);
        List<MockFlowFile> flowFiles = runner.getFlowFilesForRelationship(DeepLearning4JMultiLayerPredictor.REL_FAILURE);
        assertNotNull(flowFiles.get(0).getAttribute(DeepLearning4JMultiLayerPredictor.DEEPLEARNING4J_ERROR_MESSAGE));
    }

    @Test
    public void testMatchWithEmptyParams() {
        runner.enqueue("");
        runner.run(1,true,true);
        runner.assertAllFlowFilesTransferred(DeepLearning4JMultiLayerPredictor.REL_FAILURE, 1);
        List<MockFlowFile> flowFiles = runner.getFlowFilesForRelationship(DeepLearning4JMultiLayerPredictor.REL_FAILURE);
        assertNotNull(flowFiles.get(0).getAttribute(DeepLearning4JMultiLayerPredictor.DEEPLEARNING4J_ERROR_MESSAGE));
    }

    @Test
    public void testMatchWith5ParamOk() {
        runner.enqueue("5.1,3.5,1.4,0.2,.6");
        runner.run(1,true,true);
        runner.assertAllFlowFilesTransferred(DeepLearning4JMultiLayerPredictor.REL_SUCCESS, 1);

        List<MockFlowFile> flowFiles = runner.getFlowFilesForRelationship(DeepLearning4JMultiLayerPredictor.REL_SUCCESS);
        String shapeString = flowFiles.get(0).getAttribute(DeepLearning4JMultiLayerPredictor.DEEPLEARNING4J_OUTPUT_SHAPE);

        Integer [] shape = gson.fromJson(new StringReader(shapeString), Integer[].class);
        assertEquals("size of shape should be equal", 1, shape.length);
        assertEquals("shape should be equal", (Integer)4, shape[0]);
        assertNull(flowFiles.get(0).getAttribute(DeepLearning4JMultiLayerPredictor.DEEPLEARNING4J_ERROR_MESSAGE));

        Double[][] value = gson.fromJson(new StringReader(new String(flowFiles.get(0).toByteArray())),Double[][].class);
        assertEquals("size should be same", classificationOutputNumber, value[0].length);
    }
}
