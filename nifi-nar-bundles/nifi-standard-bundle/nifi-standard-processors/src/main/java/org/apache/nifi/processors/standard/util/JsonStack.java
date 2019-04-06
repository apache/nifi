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
package org.apache.nifi.processors.standard.util;

import org.apache.nifi.processors.standard.SplitLargeJson;

import javax.json.stream.JsonParser.Event;
import java.util.ArrayDeque;
import java.util.Iterator;

/** A <code>JsonStack</code> instance represents a location in a JSON document in a manner that is comparable
 * with a <code>SimpleJsonPath</code>.  As an extension of <code>ArrayDeque</code>, it grows and shrinks as a
 * JSON file is parsed, keeping track of only the <code>SimpleJsonPath.Element</code> instances needed to
 * "arrive at" the current location.  The <code>receive</code> method is used to convert from an instance of
 * <code>javax.json.stream.JsonParser.Event</code> to an instance of a concrete subclass of
 * <code>SimpleJsonPath.Element</code>. */
public class JsonStack extends ArrayDeque<SimpleJsonPath.Element> {

    public void receive(Event e, SplitLargeJson.JsonParserView parserView) {
        switch (e) {
            case KEY_NAME:
                add(new SimpleJsonPath.NamedElement(parserView.getString()));
                break;
            case START_ARRAY:
                add(new SimpleJsonPath.NumberedElement(0));
                break;
            case START_OBJECT:
                add(new SimpleJsonPath.ObjectElement());
                break;
            case END_OBJECT:
            case END_ARRAY:
                removeLast();
            case VALUE_STRING:
            case VALUE_NUMBER:
            case VALUE_TRUE:
            case VALUE_FALSE:
            case VALUE_NULL:
                if (!isEmpty()) {
                    SimpleJsonPath.Element last = peekLast();
                    if (last instanceof SimpleJsonPath.NumberedElement) {
                        ((SimpleJsonPath.NumberedElement) last).increment();
                    } else {
                        removeLast();
                    }
                }
                break;
        }
    }

    public boolean startsWith(SimpleJsonPath path) {
        return path.isBeginningOf(this);
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder("$");
        Iterator<SimpleJsonPath.Element> i = iterator();
        for (SimpleJsonPath.Element e : this) {
            if (e instanceof SimpleJsonPath.NumberedElement) {
                sb.append(String.format("[%d]", ((SimpleJsonPath.NumberedElement)e).getIndex()));
            } else if (e instanceof SimpleJsonPath.NamedElement){
                sb.append(((SimpleJsonPath.NamedElement)e).getName());
            } else {
                sb.append(".");
            }
        }
        return sb.toString();
    }
}
