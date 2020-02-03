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
package org.apache.nifi.processors.standard.pgp;

import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.util.TestRunner;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

class ProcessorBenchmark {
    static List<BenchmarkMeasurement> bench(String title, TestRunner runner, Relationship success, Relationship failure, BenchmarkParameters params, BenchmarkConfigurator forward, BenchmarkConfigurator reverse) throws IOException, InterruptedException {
        List<BenchmarkMeasurement> results = new ArrayList<>();
        int flowBodySize = 1024 * 1024;
        byte[] body = Random.randomBytes(flowBodySize);

        int benchmarkRuns = 10;  // number of times to run the benchmark
        int warmUpRuns = 3;      // number of initial runs to discard
        int testIterations = 100; // number of iterations for the TestRunner

        for (int i = 0; i < benchmarkRuns + warmUpRuns; i++) {
            for (String paramsTitle : params.get().keySet()) {
                Map<PropertyDescriptor, String> paramsMap = params.get().get(paramsTitle);

                // setup the forward operation
                forward.setup(runner, paramsMap);
                runner.setThreadCount(1);
                runner.enqueue(body);
                runner.clearTransferState();

                // time the flow
                long testStarted = System.nanoTime();
                runner.run(testIterations);
                runner.assertAllFlowFilesTransferred(success, 1);
                runner.assertQueueEmpty();

                // setup and perform the reverse operation
                if (reverse != null) {
                    reverse.setup(runner, paramsMap);
                    runner.enqueue(runner.getFlowFilesForRelationship(success).get(0));
                    runner.clearTransferState();
                    runner.run(1);
                    runner.assertAllFlowFilesTransferred(success, 1);
                    runner.getFlowFilesForRelationship(success).get(0).assertContentEquals(body);
                }

                if (i >= warmUpRuns) {
                    results.add(new BenchmarkMeasurement(title, paramsTitle, i-warmUpRuns, flowBodySize, System.nanoTime() - testStarted));
                }
            }
        }
        return results;
    }

    static void report(List<BenchmarkMeasurement> results) {
        Map<String, List<BenchmarkMeasurement>> byParam = results.stream().collect(Collectors.groupingBy(BenchmarkMeasurement::getParam));

        for (String s : byParam.keySet()) {
            long bestTime = Long.MAX_VALUE;
            long worstTime = Long.MIN_VALUE;
            long bestRate = 0;

            int totalMs = 0;
            int totalRuns = 0;

            System.out.println(BenchmarkMeasurement.getHeader());

            for (BenchmarkMeasurement result : byParam.get(s)) {
                System.out.println(result.getSummary());

                long dur = result.duration / 1000000;
                if (dur < bestTime) {
                    bestTime = dur;
                    bestRate = result.size / dur;
                }

                if (dur > worstTime) {
                    worstTime = dur;
                }

                totalMs += dur;
                totalRuns += 1;
            }

            System.out.println(BenchmarkMeasurement.getSeparator());
            System.out.println(String.format("fastest: %,d ms @ %,d b/ms | slowest: %,d ms | mean: %,d ms", bestTime, bestRate, worstTime, totalMs/totalRuns));
            System.out.println();
        }
    }

    static void run(String title, TestRunner runner, Relationship success, Relationship failure, BenchmarkParameters params, BenchmarkConfigurator forward, BenchmarkConfigurator reverse) throws IOException, InterruptedException {
        report(bench(title, runner, success, failure, params, forward, reverse));
    }

    static interface BenchmarkParameters {
        Map<String, Map<PropertyDescriptor, String>> get();
    }

    static interface BenchmarkConfigurator {
        void setup(TestRunner runner, Map<PropertyDescriptor, String> config);
    }

    static class BenchmarkMeasurement {
        final String title;
        final String param;
        final int run;
        final int size;
        final Long duration;

        BenchmarkMeasurement(String title, String param, int run, int size, Long duration) {
            this.title = title;
            this.param = param;
            this.run = run;
            this.size = size;
            this.duration = duration;
        }

        String getParam() {
            return param;
        }

        public String getSummary() {
            long ms = duration / 1000000;
            return String.format("%-3s %-20s %-20s %6s ms %10s b/ms", run + 1, title, param, ms, size/ms);
        }

        public static String getSeparator() {
            return "-----------------------------------------------------------------------";
        }

        public static String getDoubleLine() {
            return getSeparator().replaceAll("-", "=");
        }

        public static String getHeader() {
            return String.format("%-3s %-20s %-20s %9s %14s\n%s", "#", "Test", "Parameter", "Duration", "Rate", getDoubleLine());
        }
    }
}
