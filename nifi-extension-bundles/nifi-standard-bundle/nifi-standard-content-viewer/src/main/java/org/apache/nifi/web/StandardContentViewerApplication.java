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
package org.apache.nifi.web;

import org.apache.nifi.web.servlet.filter.QueryStringToFragmentFilter;
import org.springframework.boot.Banner;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.autoconfigure.web.servlet.error.ErrorMvcAutoConfiguration;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.boot.logging.LoggingSystem;
import org.springframework.boot.web.servlet.FilterRegistrationBean;
import org.springframework.boot.web.servlet.support.SpringBootServletInitializer;
import org.springframework.context.annotation.Bean;

@SpringBootApplication(exclude = {ErrorMvcAutoConfiguration.class})
public class StandardContentViewerApplication extends SpringBootServletInitializer {

    static {
        // Disable Spring Boot logging initialization
        System.setProperty(LoggingSystem.SYSTEM_PROPERTY, LoggingSystem.NONE);
    }

    @Bean
    public FilterRegistrationBean<QueryStringToFragmentFilter> queryStringToFragmentFilter() {
        final FilterRegistrationBean<QueryStringToFragmentFilter> registration = new FilterRegistrationBean<>();
        registration.setFilter(new QueryStringToFragmentFilter());
        registration.addUrlPatterns("");
        registration.setName(QueryStringToFragmentFilter.class.getSimpleName());
        registration.setOrder(1);
        return registration;
    }

    @Override
    protected SpringApplicationBuilder createSpringApplicationBuilder() {
        return new SpringApplicationBuilder().bannerMode(Banner.Mode.OFF);
    }
}
