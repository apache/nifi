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
package org.apache.nifi.registry.extension;

import org.apache.commons.lang3.Validate;

import javax.net.ssl.SSLContext;

/**
 * Base class for implementations of ExtensionRegistry.
 *
 * @param <T> the type of bundle metadata
 */
public abstract class AbstractExtensionRegistry<T extends ExtensionBundleMetadata> implements ExtensionRegistry<T> {

    private final String identifier;
    private final SSLContext sslContext;

    private volatile String url;
    private volatile String name;
    private volatile String description;

    public AbstractExtensionRegistry(final String identifier, final String url, final String name, final SSLContext sslContext) {
        this.identifier = Validate.notBlank(identifier);
        this.url = url;
        this.name = name;
        this.sslContext = sslContext;
    }

    @Override
    public String getIdentifier() {
        return identifier;
    }

    @Override
    public String getDescription() {
        return description;
    }

    @Override
    public void setDescription(String description) {
        this.description = description;
    }

    @Override
    public String getURL() {
        return url;
    }

    @Override
    public void setURL(String url) {
        this.url = url;
    }

    @Override
    public String getName() {
        return name;
    }

    @Override
    public void setName(final String name) {
        this.name = name;
    }

    protected SSLContext getSSLContext() {
        return this.sslContext;
    }

}