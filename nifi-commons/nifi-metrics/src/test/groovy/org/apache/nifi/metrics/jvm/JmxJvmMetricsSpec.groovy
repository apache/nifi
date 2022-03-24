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
package org.apache.nifi.metrics.jvm

import org.apache.nifi.processor.DataUnit
import spock.lang.Specification
import spock.lang.Unroll

class JmxJvmMetricsSpec extends Specification {

    @Unroll
    def "Get numeric metric for #metricName via method #methodName"() {
        given:
        def jmxJvmMetrics = JmxJvmMetrics.instance

        when:
        def metricResult = jmxJvmMetrics."$methodName"(dataUnit).doubleValue()

        then:
        noExceptionThrown()
        metricResult != -1


        where:
        metricName               | methodName       | dataUnit
        "memory.total.init"      | "totalInit"      | DataUnit.B
        "memory.total.used"      | "totalUsed"      | DataUnit.B
        "memory.total.max"       | "totalMax"       | DataUnit.B
        "memory.total.committed" | "totalCommitted" | DataUnit.B
        "memory.heap.init"       | "heapInit"       | DataUnit.B
        "memory.heap.used"       | "heapUsed"       | DataUnit.B
        "memory.heap.max"        | "heapMax"        | DataUnit.B
        "memory.heap.committed"  | "heapCommitted"  | DataUnit.B
        "memory.total.init"      | "totalInit"      | DataUnit.B
        "memory.total.init"      | "totalInit"      | DataUnit.B
        "memory.total.init"      | "totalInit"      | DataUnit.B
    }

    @Unroll
    def "Get percentage metric for #metricName via method #methodName"() {
        given:
        def jmxJvmMetrics = JmxJvmMetrics.instance

        when:
        def metricResult = jmxJvmMetrics."$methodName"()

        then:
        noExceptionThrown()
        metricResult instanceof Double
        metricResult != 0.0

        where:
        metricName                | methodName
        "memory.heap.usage"       | "heapUsage"
        "memory.non-heap.usage"   | "nonHeapUsage"
        "os.filedescriptor.usage" | "fileDescriptorUsage"
    }

    def "Memory pool metric names exist"() {
        given:
        def jmxJvmMetrics = JmxJvmMetrics.instance

        when:
        def names = jmxJvmMetrics.getMetricNames(JmxJvmMetrics.REGISTRY_METRICSET_MEMORY + ".pools")

        then:
        names.size() > 0
    }

    @Unroll
    def "Get string map metric for #metricName via method #methodName"() {
        given:
        def jmxJvmMetrics = JmxJvmMetrics.instance

        when:
        def metricResult = jmxJvmMetrics."$methodName"()

        then:
        noExceptionThrown()
        metricResult.keySet().size() > 0

        where:
        metricName           | methodName
        "memory.pools.usage" | "memoryPoolUsage"
        "garbage-collectors" | "garbageCollectors"
    }
}
