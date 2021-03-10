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

package org.apache.nifi.components.resource;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.nio.charset.StandardCharsets;

public class Utf8TextResource implements ResourceReference {
    private final String text;

    public Utf8TextResource(final String text) {
        this.text = text;
    }

    @Override
    public File asFile() {
        return null;
    }

    @Override
    public URL asURL() {
        return null;
    }

    @Override
    public InputStream read() throws IOException {
        return new ByteArrayInputStream(text.getBytes(StandardCharsets.UTF_8));
    }

    @Override
    public boolean isAccessible() {
        return true;
    }

    @Override
    public String getLocation() {
        return null;
    }

    @Override
    public ResourceType getResourceType() {
        return ResourceType.TEXT;
    }

    @Override
    public String toString() {
        return "Utf8TextResource[text=" + text.length() + " characters]";
    }
}
