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
package org.apache.nifi.xml.inference;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.function.Consumer;

public class XmlArrayNode implements XmlNode {
    private final String nodeName;
    private final List<XmlNode> elements = new ArrayList<>();

    public XmlArrayNode(final String nodeName) {
        this.nodeName = nodeName;
    }

    @Override
    public XmlNodeType getNodeType() {
        return XmlNodeType.ARRAY;
    }

    void addElement(final XmlNode node) {
        elements.add(node);
    }

    public List<XmlNode> getElements() {
        return Collections.unmodifiableList(elements);
    }

    public void forEach(final Consumer<XmlNode> consumer) {
        elements.forEach(consumer);
    }

    @Override
    public String getName() {
        return nodeName;
    }

}
