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
package org.apache.nifi.framework.configuration;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.scheduling.TaskScheduler;
import org.springframework.scheduling.concurrent.ThreadPoolTaskScheduler;

/**
 * Task Scheduler Configuration class for Spring Application
 */
@Configuration
public class TaskSchedulerConfiguration {
    private static final String APPLICATION_THREAD_NAME_PREFIX = "ApplicationThread-";

    /**
     * Application Task Scheduler configured with Virtual Threads for infrequent operations
     *
     * @return Task Scheduler
     */
    @Bean
    public TaskScheduler taskScheduler() {
        final ThreadPoolTaskScheduler taskScheduler = new ThreadPoolTaskScheduler();
        taskScheduler.setThreadNamePrefix(APPLICATION_THREAD_NAME_PREFIX);
        taskScheduler.setVirtualThreads(true);
        return taskScheduler;
    }
}
