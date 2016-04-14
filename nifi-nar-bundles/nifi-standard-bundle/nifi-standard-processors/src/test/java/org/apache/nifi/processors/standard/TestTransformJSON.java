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
package org.apache.nifi.processors.standard;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.List;
import java.util.Set;

import org.apache.nifi.flowfile.attributes.CoreAttributes;
import org.apache.nifi.processor.Processor;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.stream.io.ByteArrayInputStream;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.StringUtils;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.Test;

import com.bazaarvoice.jolt.CardinalityTransform;
import com.bazaarvoice.jolt.Chainr;
import com.bazaarvoice.jolt.Defaultr;
import com.bazaarvoice.jolt.Diffy;
import com.bazaarvoice.jolt.JsonUtils;
import com.bazaarvoice.jolt.Removr;
import com.bazaarvoice.jolt.Shiftr;
import com.bazaarvoice.jolt.Sortr;
import com.bazaarvoice.jolt.Transform;

import static org.junit.Assert.assertTrue;

public class TestTransformJSON {

    final static Path JSON_INPUT = Paths.get("src/test/resources/TestTransformJson/input.json");
    final static Diffy DIFFY = new Diffy();

    @Test
    public void testRelationshipsCreated() throws IOException{
        Processor processor= new TransformJSON();
        final TestRunner runner = TestRunners.newTestRunner(processor);
        final String spec = new String(Files.readAllBytes(Paths.get("src/test/resources/TestTransformJson/chainrSpec.json")));
        runner.setProperty(TransformJSON.JOLT_SPEC, spec);
        runner.enqueue(JSON_INPUT);
        Set<Relationship> relationships = processor.getRelationships();
        assertTrue(relationships.contains(TransformJSON.REL_FAILURE));
        assertTrue(relationships.contains(TransformJSON.REL_SUCCESS));
        assertTrue(relationships.size() == 2);
    }

    @Test
    public void testInvalidJOLTSpec() throws IOException {
        final TestRunner runner = TestRunners.newTestRunner(new TransformJSON());
        final String spec = "[{}]";
        runner.setProperty(TransformJSON.JOLT_SPEC, spec);
        runner.assertNotValid();
    }

    @Test
    public void testIncorrectJOLTSpec() throws IOException {
        final TestRunner runner = TestRunners.newTestRunner(new TransformJSON());
        final String chainrSpec = new String(Files.readAllBytes(Paths.get("src/test/resources/TestTransformJson/chainrSpec.json")));
        runner.setProperty(TransformJSON.JOLT_SPEC, chainrSpec);
        runner.setProperty(TransformJSON.JOLT_TRANSFORM, TransformJSON.SHIFTR);
        runner.assertNotValid();
    }

    @Test
    public void testSpecIsNotSet() throws IOException {
        final TestRunner runner = TestRunners.newTestRunner(new TransformJSON());
        runner.setProperty(TransformJSON.JOLT_TRANSFORM, TransformJSON.SHIFTR);
        runner.assertNotValid();
    }

    @Test
    public void testSpecIsEmpty() throws IOException {
        final TestRunner runner = TestRunners.newTestRunner(new TransformJSON());
        runner.setProperty(TransformJSON.JOLT_SPEC, StringUtils.EMPTY);
        runner.setProperty(TransformJSON.JOLT_TRANSFORM, TransformJSON.SHIFTR);
        runner.assertNotValid();
    }

    @Test
    public void testSpecNotRequired() throws IOException {
        final TestRunner runner = TestRunners.newTestRunner(new TransformJSON());
        runner.setProperty(TransformJSON.JOLT_TRANSFORM, TransformJSON.SORTR);
        runner.assertValid();
    }

    @Test
    public void testNoFlowFileContent() throws IOException {
        final TestRunner runner = TestRunners.newTestRunner(new TransformJSON());
        final String spec = new String(Files.readAllBytes(Paths.get("src/test/resources/TestTransformJson/chainrSpec.json")));
        runner.setProperty(TransformJSON.JOLT_SPEC, spec);
        runner.run();
        runner.assertQueueEmpty();
        runner.assertTransferCount(TransformJSON.REL_FAILURE,0);
        runner.assertTransferCount(TransformJSON.REL_SUCCESS,0);
    }

    @Test
    public void testInvalidFlowFileContentJson() throws IOException {
        final TestRunner runner = TestRunners.newTestRunner(new TransformJSON());
        final String spec = new String(Files.readAllBytes(Paths.get("src/test/resources/TestTransformJson/chainrSpec.json")));
        runner.setProperty(TransformJSON.JOLT_SPEC, spec);
        runner.enqueue("invalid json");
        runner.run();
        runner.assertAllFlowFilesTransferred(TransformJSON.REL_FAILURE);
    }

    @Test
    @SuppressWarnings("unchecked")
    public void testGetChainTransform() throws NoSuchMethodException, IOException,InvocationTargetException, IllegalAccessException{

        final String chainrSpec = new String(Files.readAllBytes(Paths.get("src/test/resources/TestTransformJson/chainrSpec.json")));
        List<Class> classes = Arrays.<Class>asList(TransformJSON.class.getDeclaredClasses());
        Class factory = classes.stream().filter(clazz -> clazz.getSimpleName().equals("TransformationFactory")).findFirst().get();
        Method method = factory.getDeclaredMethod("getTransform", String.class, Object.class);
        method.setAccessible(true);
        Transform transform = (Transform) method.invoke(null,TransformJSON.CHAINR.getValue(),JsonUtils.jsonToObject(chainrSpec));
        assertTrue(transform instanceof Chainr);

    }

    @Test
    @SuppressWarnings("unchecked")
    public void testGetRemoveTransform() throws NoSuchMethodException, IOException,InvocationTargetException, IllegalAccessException{

        final String removrSpec = new String(Files.readAllBytes(Paths.get("src/test/resources/TestTransformJson/removrSpec.json")));
        List<Class> classes = Arrays.<Class>asList(TransformJSON.class.getDeclaredClasses());
        Class factory = classes.stream().filter(clazz -> clazz.getSimpleName().equals("TransformationFactory")).findFirst().get();
        Method method = factory.getDeclaredMethod("getTransform", String.class, Object.class);
        method.setAccessible(true);
        Transform transform = (Transform) method.invoke(null,TransformJSON.REMOVR.getValue(),JsonUtils.jsonToObject(removrSpec));
        assertTrue(transform instanceof Removr);

    }

    @Test
    @SuppressWarnings("unchecked")
    public void testGetShiftTransform() throws NoSuchMethodException, IOException,InvocationTargetException, IllegalAccessException{

        final String shiftrSpec = new String(Files.readAllBytes(Paths.get("src/test/resources/TestTransformJson/shiftrSpec.json")));
        List<Class> classes = Arrays.<Class>asList(TransformJSON.class.getDeclaredClasses());
        Class factory = classes.stream().filter(clazz -> clazz.getSimpleName().equals("TransformationFactory")).findFirst().get();
        Method method = factory.getDeclaredMethod("getTransform", String.class, Object.class);
        method.setAccessible(true);
        Transform transform = (Transform) method.invoke(null,TransformJSON.SHIFTR.getValue(),JsonUtils.jsonToObject(shiftrSpec));
        assertTrue(transform instanceof Shiftr);
    }

    @Test
    @SuppressWarnings("unchecked")
    public void testGetDefaultTransform() throws NoSuchMethodException, IOException,InvocationTargetException, IllegalAccessException{

        final String defaultrSpec = new String(Files.readAllBytes(Paths.get("src/test/resources/TestTransformJson/defaultrSpec.json")));
        List<Class> classes = Arrays.<Class>asList(TransformJSON.class.getDeclaredClasses());
        Class factory = classes.stream().filter(clazz -> clazz.getSimpleName().equals("TransformationFactory")).findFirst().get();
        Method method = factory.getDeclaredMethod("getTransform", String.class, Object.class);
        method.setAccessible(true);
        Transform transform = (Transform) method.invoke(null,TransformJSON.DEFAULTR.getValue(),JsonUtils.jsonToObject(defaultrSpec));
        assertTrue(transform instanceof Defaultr);
    }

    @Test
    @SuppressWarnings("unchecked")
    public void testGetCardinalityTransform() throws NoSuchMethodException, IOException,InvocationTargetException, IllegalAccessException{

        final String cardrSpec = new String(Files.readAllBytes(Paths.get("src/test/resources/TestTransformJson/cardrSpec.json")));
        List<Class> classes = Arrays.<Class>asList(TransformJSON.class.getDeclaredClasses());
        Class factory = classes.stream().filter(clazz -> clazz.getSimpleName().equals("TransformationFactory")).findFirst().get();
        Method method = factory.getDeclaredMethod("getTransform", String.class, Object.class);
        method.setAccessible(true);
        Transform transform = (Transform) method.invoke(null,TransformJSON.CARDINALITY.getValue(),JsonUtils.jsonToObject(cardrSpec));
        assertTrue(transform instanceof CardinalityTransform);
    }

    @Test
    @SuppressWarnings("unchecked")
    public void testGetSortrTransform() throws NoSuchMethodException, IOException,InvocationTargetException, IllegalAccessException{
        List<Class> classes = Arrays.<Class>asList(TransformJSON.class.getDeclaredClasses());
        Class factory = classes.stream().filter(clazz -> clazz.getSimpleName().equals("TransformationFactory")).findFirst().get();
        Method method = factory.getDeclaredMethod("getTransform", String.class, Object.class);
        method.setAccessible(true);
        Transform transform = (Transform) method.invoke(null,TransformJSON.SORTR.getValue(),null);
        assertTrue(transform instanceof Sortr);
    }


    @Test
    @SuppressWarnings("unchecked")
    public void testGetInvalidTransformWithNoSpec() throws NoSuchMethodException, IOException,InvocationTargetException, IllegalAccessException{
        List<Class> classes = Arrays.<Class>asList(TransformJSON.class.getDeclaredClasses());
        Class factory = classes.stream().filter(clazz -> clazz.getSimpleName().equals("TransformationFactory")).findFirst().get();
        Method method = factory.getDeclaredMethod("getTransform", String.class, Object.class);
        method.setAccessible(true);
        try {
            method.invoke(null, TransformJSON.CHAINR.getValue(), null);
        }catch (InvocationTargetException ite){
            assertTrue(ite.getTargetException().getLocalizedMessage().equals("JOLT Chainr expects a JSON array of objects - Malformed spec."));
        }
    }

    @Test
    public void testTransformInputWithChainr() throws IOException {
        final TestRunner runner = TestRunners.newTestRunner(new TransformJSON());
        final String spec = new String(Files.readAllBytes(Paths.get("src/test/resources/TestTransformJson/chainrSpec.json")));
        runner.setProperty(TransformJSON.JOLT_SPEC, spec);
        runner.enqueue(JSON_INPUT);
        runner.run();
        runner.assertAllFlowFilesTransferred(TransformJSON.REL_SUCCESS);
        final MockFlowFile transformed = runner.getFlowFilesForRelationship(TransformJSON.REL_SUCCESS).get(0);
        transformed.assertAttributeExists(CoreAttributes.MIME_TYPE.key());
        transformed.assertAttributeEquals(CoreAttributes.MIME_TYPE.key(),"application/json");
        Object transformedJson = JsonUtils.jsonToObject(new ByteArrayInputStream(transformed.toByteArray()));
        Object compareJson = JsonUtils.jsonToObject(Files.newInputStream(Paths.get("src/test/resources/TestTransformJson/chainrOutput.json")));
        assertTrue(DIFFY.diff(compareJson, transformedJson).isEmpty());
    }

    @Test
    public void testTransformInputWithShiftr() throws IOException {
        final TestRunner runner = TestRunners.newTestRunner(new TransformJSON());
        final String spec = new String(Files.readAllBytes(Paths.get("src/test/resources/TestTransformJson/shiftrSpec.json")));
        runner.setProperty(TransformJSON.JOLT_SPEC, spec);
        runner.setProperty(TransformJSON.JOLT_TRANSFORM, TransformJSON.SHIFTR);
        runner.enqueue(JSON_INPUT);
        runner.run();
        runner.assertAllFlowFilesTransferred(TransformJSON.REL_SUCCESS);
        final MockFlowFile transformed = runner.getFlowFilesForRelationship(TransformJSON.REL_SUCCESS).get(0);
        transformed.assertAttributeExists(CoreAttributes.MIME_TYPE.key());
        transformed.assertAttributeEquals(CoreAttributes.MIME_TYPE.key(),"application/json");
        Object transformedJson = JsonUtils.jsonToObject(new ByteArrayInputStream(transformed.toByteArray()));
        Object compareJson = JsonUtils.jsonToObject(Files.newInputStream(Paths.get("src/test/resources/TestTransformJson/shiftrOutput.json")));
        assertTrue(DIFFY.diff(compareJson, transformedJson).isEmpty());
    }

    @Test
    public void testTransformInputWithDefaultr() throws IOException {
        final TestRunner runner = TestRunners.newTestRunner(new TransformJSON());
        final String spec = new String(Files.readAllBytes(Paths.get("src/test/resources/TestTransformJson/defaultrSpec.json")));
        runner.setProperty(TransformJSON.JOLT_SPEC, spec);
        runner.setProperty(TransformJSON.JOLT_TRANSFORM, TransformJSON.DEFAULTR);
        runner.enqueue(JSON_INPUT);
        runner.run();
        runner.assertAllFlowFilesTransferred(TransformJSON.REL_SUCCESS);
        final MockFlowFile transformed = runner.getFlowFilesForRelationship(TransformJSON.REL_SUCCESS).get(0);
        Object transformedJson = JsonUtils.jsonToObject(new ByteArrayInputStream(transformed.toByteArray()));
        Object compareJson = JsonUtils.jsonToObject(Files.newInputStream(Paths.get("src/test/resources/TestTransformJson/defaultrOutput.json")));
        assertTrue(DIFFY.diff(compareJson, transformedJson).isEmpty());
    }

    @Test
    public void testTransformInputWithRemovr() throws IOException {
        final TestRunner runner = TestRunners.newTestRunner(new TransformJSON());
        final String spec = new String(Files.readAllBytes(Paths.get("src/test/resources/TestTransformJson/removrSpec.json")));
        runner.setProperty(TransformJSON.JOLT_SPEC, spec);
        runner.setProperty(TransformJSON.JOLT_TRANSFORM, TransformJSON.REMOVR);
        runner.enqueue(JSON_INPUT);
        runner.run();
        runner.assertAllFlowFilesTransferred(TransformJSON.REL_SUCCESS);
        final MockFlowFile transformed = runner.getFlowFilesForRelationship(TransformJSON.REL_SUCCESS).get(0);
        Object transformedJson = JsonUtils.jsonToObject(new ByteArrayInputStream(transformed.toByteArray()));
        Object compareJson = JsonUtils.jsonToObject(Files.newInputStream(Paths.get("src/test/resources/TestTransformJson/removrOutput.json")));
        assertTrue(DIFFY.diff(compareJson, transformedJson).isEmpty());
    }

    @Test
    public void testTransformInputWithCardinality() throws IOException {
        final TestRunner runner = TestRunners.newTestRunner(new TransformJSON());
        final String spec = new String(Files.readAllBytes(Paths.get("src/test/resources/TestTransformJson/cardrSpec.json")));
        runner.setProperty(TransformJSON.JOLT_SPEC, spec);
        runner.setProperty(TransformJSON.JOLT_TRANSFORM, TransformJSON.CARDINALITY);
        runner.enqueue(JSON_INPUT);
        runner.run();
        runner.assertAllFlowFilesTransferred(TransformJSON.REL_SUCCESS);
        final MockFlowFile transformed = runner.getFlowFilesForRelationship(TransformJSON.REL_SUCCESS).get(0);
        Object transformedJson = JsonUtils.jsonToObject(new ByteArrayInputStream(transformed.toByteArray()));
        Object compareJson = JsonUtils.jsonToObject(Files.newInputStream(Paths.get("src/test/resources/TestTransformJson/cardrOutput.json")));
        assertTrue(DIFFY.diff(compareJson, transformedJson).isEmpty());
    }

    @Test
    public void testTransformInputWithSortr() throws IOException {
        final TestRunner runner = TestRunners.newTestRunner(new TransformJSON());
        runner.setProperty(TransformJSON.JOLT_TRANSFORM, TransformJSON.SORTR);
        runner.enqueue(JSON_INPUT);
        runner.run();
        runner.assertAllFlowFilesTransferred(TransformJSON.REL_SUCCESS);
        final MockFlowFile transformed = runner.getFlowFilesForRelationship(TransformJSON.REL_SUCCESS).get(0);
        transformed.assertAttributeExists(CoreAttributes.MIME_TYPE.key());
        transformed.assertAttributeEquals(CoreAttributes.MIME_TYPE.key(),"application/json");
        Object transformedJson = JsonUtils.jsonToObject(new ByteArrayInputStream(transformed.toByteArray()));
        Object compareJson = JsonUtils.jsonToObject(Files.newInputStream(Paths.get("src/test/resources/TestTransformJson/sortrOutput.json")));
        String transformedJsonString = JsonUtils.toJsonString(transformedJson);
        String compareJsonString = JsonUtils.toJsonString(compareJson);
        assertTrue(compareJsonString.equals(transformedJsonString));
    }


}
