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
package org.apache.nifi.controller;

import org.apache.nifi.bundle.BundleCoordinate;
import org.apache.nifi.components.ConfigurableComponent;
import org.apache.nifi.logging.ComponentLog;

/**
 * Holder to pass around a ConfigurableComponent with its coordinate and logger.
 *
 * @param <T> the type of ConfigurableComponent
 */
public class LoggableComponent<T extends ConfigurableComponent> {

    private final T component;

    private final BundleCoordinate bundleCoordinate;

    private final ComponentLog logger;

    public LoggableComponent(final T component, final BundleCoordinate bundleCoordinate, final ComponentLog logger) {
        this.component = component;
        this.bundleCoordinate = bundleCoordinate;
        this.logger = logger;
    }

    public T getComponent() {
        return component;
    }

    public BundleCoordinate getBundleCoordinate() {
        return bundleCoordinate;
    }

    public ComponentLog getLogger() {
        return logger;
    }

}
