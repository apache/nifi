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
package org.apache.nifi.hl7.hapi;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.nifi.hl7.model.HL7Component;
import org.apache.nifi.hl7.model.HL7Field;

import ca.uhn.hl7v2.model.Composite;
import ca.uhn.hl7v2.model.ExtraComponents;
import ca.uhn.hl7v2.model.Primitive;
import ca.uhn.hl7v2.model.Type;
import ca.uhn.hl7v2.parser.EncodingCharacters;
import ca.uhn.hl7v2.parser.PipeParser;

public class HapiField implements HL7Field, HL7Component {

    private final String value;
    private final List<HL7Component> components;

    public HapiField(final Type type) {
        this.value = PipeParser.encode(type, EncodingCharacters.defaultInstance());

        final List<HL7Component> componentList = new ArrayList<>();
        if (type instanceof Composite) {
            final Composite composite = (Composite) type;

            for (final Type component : composite.getComponents()) {
                componentList.add(new HapiField(component));
            }
        }

        final ExtraComponents extra = type.getExtraComponents();
        if (extra != null && extra.numComponents() > 0) {
            final String singleFieldValue;
            if (type instanceof Primitive) {
                singleFieldValue = ((Primitive) type).getValue();
            } else {
                singleFieldValue = this.value;
            }
            componentList.add(new SingleValueField(singleFieldValue));

            for (int i = 0; i < extra.numComponents(); i++) {
                componentList.add(new HapiField(extra.getComponent(i)));
            }
        }

        this.components = Collections.unmodifiableList(componentList);
    }

    @Override
    public String getValue() {
        return value;
    }

    @Override
    public List<HL7Component> getComponents() {
        return components;
    }

    @Override
    public String toString() {
        return value;
    }
}
