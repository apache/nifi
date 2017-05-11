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
package org.apache.nifi.controller.label;

import org.apache.nifi.authorization.Resource;
import org.apache.nifi.authorization.resource.Authorizable;
import org.apache.nifi.authorization.resource.ResourceFactory;
import org.apache.nifi.authorization.resource.ResourceType;
import org.apache.nifi.connectable.Position;
import org.apache.nifi.connectable.Size;
import org.apache.nifi.groups.ProcessGroup;
import org.apache.nifi.util.CharacterFilterUtils;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

public class StandardLabel implements Label {

    private final String identifier;
    private final AtomicReference<Position> position;
    private final AtomicReference<Size> size;
    private final AtomicReference<Map<String, String>> style;
    private final AtomicReference<String> value;
    private final AtomicReference<ProcessGroup> processGroup;

    public StandardLabel(final String identifier, final String value) {
        this(identifier, new Position(0D, 0D), new HashMap<String, String>(), value, null);
    }

    public StandardLabel(final String identifier, final Position position, final Map<String, String> style, final String value, final ProcessGroup processGroup) {
        this.identifier = identifier;
        this.position = new AtomicReference<>(position);
        this.style = new AtomicReference<>(Collections.unmodifiableMap(new HashMap<>(style)));
        this.size = new AtomicReference<>(new Size(150, 150));
        this.value = new AtomicReference<>(CharacterFilterUtils.filterInvalidXmlCharacters(value));
        this.processGroup = new AtomicReference<>(processGroup);
    }

    @Override
    public Position getPosition() {
        return position.get();
    }

    @Override
    public void setPosition(final Position position) {
        if (position != null) {
            this.position.set(position);
        }
    }

    @Override
    public Size getSize() {
        return size.get();
    }

    @Override
    public void setSize(final Size size) {
        if (size != null) {
            this.size.set(size);
        }
    }

    public String getIdentifier() {
        return identifier;
    }

    @Override
    public String getProcessGroupIdentifier() {
        final ProcessGroup procGroup = getProcessGroup();
        return procGroup == null ? null : procGroup.getIdentifier();
    }

    @Override
    public Authorizable getParentAuthorizable() {
        return getProcessGroup();
    }

    @Override
    public Resource getResource() {
        return ResourceFactory.getComponentResource(ResourceType.Label, getIdentifier(),"Label");
    }

    public Map<String, String> getStyle() {
        return style.get();
    }

    public void setStyle(final Map<String, String> style) {
        if (style != null) {
            boolean updated = false;
            while (!updated) {
                final Map<String, String> existingStyles = this.style.get();
                final Map<String, String> updatedStyles = new HashMap<>(existingStyles);
                updatedStyles.putAll(style);
                updated = this.style.compareAndSet(existingStyles, Collections.unmodifiableMap(updatedStyles));
            }
        }
    }

    public String getValue() {
        return value.get();
    }

    public void setValue(final String value) {
        this.value.set(CharacterFilterUtils.filterInvalidXmlCharacters(value));
    }

    public void setProcessGroup(final ProcessGroup group) {
        this.processGroup.set(group);
    }

    public ProcessGroup getProcessGroup() {
        return processGroup.get();
    }
}
