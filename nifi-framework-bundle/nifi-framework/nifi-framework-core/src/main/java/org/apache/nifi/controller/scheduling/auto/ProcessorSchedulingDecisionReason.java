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
package org.apache.nifi.controller.scheduling.auto;

public enum ProcessorSchedulingDecisionReason {
    COLLECTING_COMPARISON_MEASUREMENTS,
    TESTING_HIGHER_CONCURRENCY,
    TESTING_LOWER_CONCURRENCY,
    HIGHER_CONCURRENCY_INCREASED_THROUGHPUT,
    HIGHER_CONCURRENCY_DID_NOT_INCREASE_THROUGHPUT,
    LOWER_CONCURRENCY_MAINTAINED_THROUGHPUT,
    LOWER_CONCURRENCY_REDUCED_THROUGHPUT_OR_INCREASED_INPUT_QUEUE,
    COMPARISON_EXPIRED_WITHOUT_ENOUGH_MEASUREMENTS,
    CPU_LOAD_TOO_HIGH,
    GARBAGE_COLLECTION_TIME_TOO_HIGH,
    PROCESSOR_INVOCATION_FAILED,
    GLOBAL_CAPACITY_FULL,
    NO_WAITING_WORK,
    REDUCED_UNUSED_CONCURRENT_TASK,
    WAITING_BEFORE_ANOTHER_INCREASE,
    CONCURRENT_TASKS_EXCEEDED_CURRENT_LIMIT,
    PERFORMANCE_MEASUREMENTS_RESET,
    PROCESSOR_BLOCKED_BY_BACK_PRESSURE,
    PROCESSOR_YIELDING
}
