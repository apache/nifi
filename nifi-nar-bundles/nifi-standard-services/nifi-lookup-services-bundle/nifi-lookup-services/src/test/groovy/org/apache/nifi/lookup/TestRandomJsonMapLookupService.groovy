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

package org.apache.nifi.lookup

import org.apache.nifi.components.PropertyDescriptor
import org.apache.nifi.processor.AbstractProcessor
import org.apache.nifi.processor.ProcessContext
import org.apache.nifi.processor.ProcessSession
import org.apache.nifi.processor.exception.ProcessException
import org.apache.nifi.util.TestRunners
import org.junit.Assert
import org.junit.Test

import static groovy.json.JsonOutput.prettyPrint
import static groovy.json.JsonOutput.toJson

class TestRandomJsonMapLookupService {
    static File writeTestFile(input) {
        def temp = File.createTempFile(String.valueOf(System.currentTimeMillis()), String.valueOf(System.currentTimeMillis()))
        temp.write(prettyPrint(toJson(input)))
        temp.deleteOnExit()
        temp
    }

    static File buildHashFile(int ceiling) {
        def o = [:]
        0.upto(ceiling) {
            o["test-${it}"] = UUID.randomUUID().toString()
        }

        writeTestFile(o)
    }

    static File buildListFile(int ceiling) {
        def o = []
        0.upto(ceiling - 1) {
            o << UUID.randomUUID().toString()
        }

        writeTestFile(o)
    }

    private Map buildRunner(int ceiling, String type) {
        buildRunner(ceiling, type, null)
    }

    private Map buildRunner(int ceiling, Closure builder) {
        buildRunner(ceiling, null, builder)
    }

    private Map buildRunner(int ceiling, String type, Closure closure) {
        def testData = type ? "build${type}File"(ceiling) : closure.call(ceiling)
        def service = new RandomJsonMapLookupService()
        def runner = TestRunners.newTestRunner(RandomJsonProcessor.class)
        runner.addControllerService("service", service)
        runner.setProperty(service, RandomJsonMapLookupService.FILE_PATH, testData.absolutePath)
        runner.enableControllerService(service)
        runner.setProperty("service", "service")
        [
            runner: runner,
            service: service
        ]
    }

    static final String HASH = "Hash"
    static final String LIST = "List"

    void testHash(int ceiling, int maxSeconds) {
        def setup = buildRunner(ceiling, HASH)
        setup["runner"].assertValid()
        def start = System.currentTimeSeconds()
        0.upto(5000) {
            Optional<Map> result = setup["service"].lookup([:])
            Assert.assertNotNull(result)
            Assert.assertTrue(result.isPresent())
            def r = result.get()
            Assert.assertTrue(r instanceof Map)
            Assert.assertTrue(r.keySet().iterator().next() instanceof String)
        }
        def end   = System.currentTimeSeconds()
        Assert.assertTrue(end - start < maxSeconds)
    }

    void testList(int ceiling, int maxSeconds) {
        def setup = buildRunner(ceiling, LIST)
        setup["runner"].assertValid()
        def start = System.currentTimeSeconds()
        0.upto(5000) {
            Optional<Map> result = setup["service"].lookup([:])
            Assert.assertNotNull(result)
            Assert.assertTrue(result.isPresent())
            def r = result.get()
            Assert.assertNotNull(r)
            Assert.assertTrue(r instanceof Map)
            Assert.assertTrue(r.keySet().iterator().next() instanceof String)
        }
        def end   = System.currentTimeSeconds()
        Assert.assertTrue(end - start < maxSeconds)
    }

    @Test
    void testNestedHashes() {
        def setup = buildRunner(20000) { ceiling ->
            def o = [:]
            0.upto(ceiling - 1) {
                o << [
                    msg: UUID.randomUUID().toString(),
                    currentTime: System.currentTimeMillis()
                ]
            }

            writeTestFile(o)
        }
        setup["runner"].assertValid()
        def start = System.currentTimeSeconds()
        0.upto(5000) {
            Optional<Map> result = setup["service"].lookup([:])
            Assert.assertNotNull(result)
            Assert.assertTrue(result.isPresent())
            Assert.assertNotNull(result.get())
        }
        def end   = System.currentTimeSeconds()
        Assert.assertTrue(end - start < 1)
    }

    @Test
    void testSimpleList() {
        testList(5000, 2)
    }

    @Test
    void testMediumList() {
        testList(25000, 2)
    }

    @Test
    void testLargerList() {
        testList(50000, 2)
    }

    @Test
    void testMuchLargerList() {
        testList(100000, 2)
    }

    @Test
    void testSimpleHash() {
        testHash(5000, 2)
    }

    @Test
    void testMediumHash() {
        testHash(25000, 2)
    }

    @Test
    void testLargerHash() {
        testHash(50000, 2)
    }

    @Test
    void testMuchLargerHash() {
        testHash(100000, 2)
    }
}

class RandomJsonProcessor extends AbstractProcessor {
    @Override
    List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        [
            new PropertyDescriptor.Builder()
                .name("service")
                .displayName("Service")
                .identifiesControllerService(RandomLookupService.class)
                .required(true)
                .build()
        ]
    }

    @Override
    void onTrigger(ProcessContext context, ProcessSession session) throws ProcessException {

    }
}