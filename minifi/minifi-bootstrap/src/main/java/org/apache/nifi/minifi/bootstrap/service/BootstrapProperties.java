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
package org.apache.nifi.minifi.bootstrap.service;

import static org.apache.nifi.minifi.commons.utils.PropertyUtil.resolvePropertyValue;

import java.util.Properties;

/**
 * Extends Properties functionality with System and Environment property override possibility. The property resolution also works with
 * dots and hyphens that are not supported in some shells.
 */
public class BootstrapProperties extends Properties {

    private BootstrapProperties() {
        super();
    }

    public static BootstrapProperties getInstance() {
        return new BootstrapProperties();
    }

    @Override
    public String getProperty(String key) {
        return resolvePropertyValue(key, System.getProperties())
            .or(() -> resolvePropertyValue(key, System.getenv()))
            .orElseGet(() -> super.getProperty(key));
    }

}
