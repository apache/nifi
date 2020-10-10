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
package org.apache.nifi.graph

import org.apache.nifi.processor.AbstractProcessor
import org.apache.nifi.processor.ProcessContext
import org.apache.nifi.processor.ProcessSession
import org.apache.nifi.processor.exception.ProcessException
import org.apache.nifi.util.TestRunner
import org.apache.nifi.util.TestRunners
import org.junit.After
import org.junit.Before
import org.junit.Test

class GremlinBytecodeClientServiceIT {
    TestRunner runner
    GraphClientService service

    def param

    @Before
    void before() {
        param = [
                "newVertices": [],
                "edges": [],
                "existing": [:]
        ]

        param.newVertices = [
                "left": ['label': "testlabel"],
                "right": ['label': "testlabel"]
        ]

        def edge = ['from': 'left', 'to': 'right', 'label': 'lefttoright']
        param.edges = [
                edge
        ]
        runner = TestRunners.newTestRunner(new AbstractProcessor() {
            @Override
            void onTrigger(ProcessContext processContext, ProcessSession processSession) throws ProcessException {

            }
        })

        service = new GremlinBytecodeClientService()
        runner.addControllerService("service", service)
        runner.setProperty(service, GremlinBytecodeClientService.REMOTE_OBJECTS_FILE, "src/test/resources/gremlin.yml")
        runner.enableControllerService(service)
        runner.assertValid()
    }

    @Test
    void testManualConfigurationWithProperties() {
        service = new GremlinBytecodeClientService()
        runner.addControllerService("service", service)
        runner.setProperty(service, GremlinBytecodeClientService.CONTACT_POINTS, "localhost")
        runner.setProperty(service, GremlinBytecodeClientService.PORT, "8182")
        runner.setProperty(service, GremlinBytecodeClientService.PATH, "/gremlin")
        runner.enableControllerService(service)
        runner.assertValid()
    }

    @Test
    void testBothConfigurationOptionsCannotBeEnabled() {
        runner.disableControllerService(service)
        runner.setProperty(service, GremlinBytecodeClientService.CONTACT_POINTS, "localhost")
        runner.setProperty(service, GremlinBytecodeClientService.PORT, "8182")
        runner.setProperty(service, GremlinBytecodeClientService.PATH, "/gremlin")
        testForIllegalStateException()
    }

    private void testForIllegalStateException() {
        def ex = null
        try {
            runner.enableControllerService(service)
        } catch (Exception e) {
            ex = e
        } finally {
            assert ex instanceof IllegalStateException
        }
    }

    @Test
    void testOneConfigurationChoiceMustBeMade() {
        service = new GremlinBytecodeClientService()
        runner.addControllerService("service", service)

        testForIllegalStateException()
    }

    @After
    void teardown() {
        ((GremlinBytecodeClientService)service).shutdown()
    }

    @Test
    void testQuery() {
        def query = """
            assert param
            g.V().hasLabel("nada").count().next()
        """

        service.executeQuery(query, [ param: "Hi!" ], { result, more ->
            assert result
            assert result["result"] == 0
        } as GraphQueryResultCallback)
    }

    @Test
    void testMassGenerate() {
        def query = """
            assert param
            1.upto(100) {
                def trav = g.addV("it_test_node")
                2.upto(250) {
                    trav.addV("it_test_node").property("uuid", UUID.randomUUID().toString())
                        .property("msg", param)
                }
                trav.next()
            }
            def count = g.V().hasLabel("it_test_node").count().next()
            g.V().hasLabel("it_test_node").drop().iterate()
            count
        """.trim()

        service.executeQuery(query, [ param: "Hi!" ], { result, more ->
            assert result
            assert result["result"] == 25000
        } as GraphQueryResultCallback)
    }

    @Test
    void testMultipleReturnObjects() {
        def query = """
            Map<String, Object> vertexHashes = new HashMap()
            vertexHashes.put('1', '1')
            vertexHashes.put('2', '2')
            [
              'testKey': vertexHashes,
              'result': vertexHashes
            ]
        """.trim()
        def testKeyCount = 0
        def resultKeyCount = 0
        service.executeQuery(query, param, { result, more ->
            assert result
            if (result.keySet().contains('testKey')) {
                testKeyCount++
            } else if (result.keySet().contains('result')) {
                resultKeyCount++
            }
        })
        assert testKeyCount == 2
        assert resultKeyCount == 2
    }

    @Test
    void testListReturnObject() {
        def query = """
            Map<String, Object> vertexHashes = new HashMap()
            vertexHashes.put('1', '1')
            vertexHashes.put('2', '2')
            [
              'testKey': [1,2,3],
              'result': vertexHashes
            ]
        """.trim()
        def resultKeyCount = 0
        service.executeQuery(query, param, { result, more ->
            assert result
            if (result.keySet().contains('testKey')) {
                assert result['testKey'] == [1,2,3]
            } else if (result.keySet().contains('result')) {
                resultKeyCount++
            }
        })
        assert resultKeyCount == 2
    }
}
