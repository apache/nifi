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

package org.apache.nifi.processors.evtx.parser.bxml;

import org.apache.nifi.processors.evtx.parser.BinaryReader;
import org.apache.nifi.processors.evtx.parser.BxmlNodeVisitor;
import org.apache.nifi.processors.evtx.parser.ChunkHeader;
import org.apache.nifi.processors.evtx.parser.NumberUtil;
import org.apache.nifi.processors.evtx.parser.bxml.value.VariantTypeNode;
import org.apache.nifi.processors.evtx.parser.bxml.value.VariantTypeNodeFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Node that contains children and substitutions
 */
public class RootNode extends BxmlNode {
    private final int substitutionCount;
    private final List<VariantTypeNode> substitutions;

    public RootNode(BinaryReader binaryReader, ChunkHeader chunkHeader, BxmlNode parent) throws IOException {
        super(binaryReader, chunkHeader, parent);
        init();
        substitutionCount = NumberUtil.intValueMax(binaryReader.readDWord(), Integer.MAX_VALUE, "Invalid substitution count.");
        List<VariantTypeSizeAndFactory> substitutionVariantFactories = new ArrayList<>(substitutionCount);
        for (long i = 0; i < substitutionCount; i++) {
            try {
                int substitutionSize = binaryReader.readWord();
                int substitutionType = binaryReader.readWord();
                substitutionVariantFactories.add(new VariantTypeSizeAndFactory(substitutionSize, ValueNode.factories.get(substitutionType)));
            } catch (Exception e) {
                throw new IOException(e);
            }
        }
        List<VariantTypeNode> substitutions = new ArrayList<>();
        for (VariantTypeSizeAndFactory substitutionVariantFactory : substitutionVariantFactories) {
            substitutions.add(substitutionVariantFactory.factory.create(binaryReader, chunkHeader, this, substitutionVariantFactory.size));
        }
        this.substitutions = Collections.unmodifiableList(substitutions);
    }

    @Override
    public void accept(BxmlNodeVisitor bxmlNodeVisitor) throws IOException {
        bxmlNodeVisitor.visit(this);
    }

    public List<VariantTypeNode> getSubstitutions() {
        return substitutions;
    }

    @Override
    public String toString() {
        return "RootNode{" + getChildren() + "}";
    }

    public class VariantTypeSizeAndFactory {
        private final int size;
        private final VariantTypeNodeFactory factory;

        public VariantTypeSizeAndFactory(int size, VariantTypeNodeFactory factory) {
            this.size = size;
            this.factory = factory;
        }
    }
}
