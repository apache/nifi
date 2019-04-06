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

import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.flowfile.attributes.CoreAttributes;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processors.standard.SplitLargeJson;

import javax.json.Json;
import javax.json.stream.JsonGenerator;
import javax.json.stream.JsonGeneratorFactory;
import javax.json.stream.JsonParser.Event;
import java.io.IOException;
import java.io.OutputStream;
import java.util.HashMap;
import java.util.Map;

import static org.apache.nifi.flowfile.attributes.FragmentAttributes.FRAGMENT_ID;
import static org.apache.nifi.flowfile.attributes.FragmentAttributes.FRAGMENT_INDEX;
import static org.apache.nifi.flowfile.attributes.FragmentAttributes.SEGMENT_ORIGINAL_FILENAME;

/** An instance of <code>JsonFragmentWriter</code> serves as a filter for
 * <code>javax.json.stream.JsonParser.Event</code> instances, creating new FlowFiles when necessary. The Event that
 * is passed to the <code>filterEvent</code> method can be thought of as a "cursor" passing through the original
 * JSON document.  When a fragment of interest comes into view (see comments for the <code>BoolPair</code> class),
 * a new FlowFile is created. Subsequent calls to <code>filterEvent</code> will copy the desired JSON fragment into
 * the FlowFile. When the fragment passes out of view, the FlowFile is closed and transferred, freeing up associated
 * resources from memory. */
public class JsonFragmentWriter {

    private static JsonGeneratorFactory factory = Json.createGeneratorFactory(null);
    private final ProcessSession processSession;
    private final FlowFile original;
    private final Relationship relSplit;
    private final Mode mode;
    private FlowFile fragment;
    private OutputStream outputStream;
    private JsonGenerator generator;
    private Integer targetDepth;
    private Integer prevDepth;
    private Event context;
    private String contextName;
    private Boolean addContext;
    private int fragmentIndex;
    private Map<String, String> attributes = new HashMap<>();

    private final String groupId;

    public int getCount() {
        return fragmentIndex;
    }

    public enum Mode {
        /** When a single contiguous section of a large JSON file is split into multiple FlowFiles */
        CONTIGUOUS,

        /** When multiple disjoint excerpts from a large JSON are to become separate FlowFiles */
        DISJOINT
    }

    /** This is a helper class to keep track of when a targeted section of JSON is coming into view ("waxing"),
     * in view ("full"), or going out of view ("waning"). */
    public static class BoolPair {
        private boolean previous;
        private boolean current;

        boolean waxing() {
            return !previous && current;
        }

        boolean waning() {
            return previous && !current;
        }

        boolean full() {
            return current && previous;
        }

        public void push(boolean current) {
            previous = this.current;
            this.current = current;
        }
    }

    public JsonFragmentWriter(ProcessSession processSession, FlowFile original, Relationship relSplit, String groupId, Mode mode){
        this.processSession = processSession;
        this.original = original;
        this.relSplit = relSplit;
        this.mode = mode;
        this.groupId = groupId;
        attributes.put(FRAGMENT_ID.key(), groupId);
        attributes.put(SEGMENT_ORIGINAL_FILENAME.key(), original.getAttribute(CoreAttributes.FILENAME.key()));
    }

    private void newFragment(SplitLargeJson.JsonParserView view) {
        fragment = processSession.create(original);
        outputStream = processSession.write(fragment);
        generator = factory.createGenerator(outputStream);
        if ((addContext != null) && addContext) {
            write(context, view);
        }
    }

    private void endFragment() throws IOException {
        if ((addContext != null) && addContext) {
            generator.writeEnd();
        }
        generator.flush();
        generator.close();
        generator = null;
        outputStream.close();
        addContext = null;
        attributes.put(FRAGMENT_INDEX.key(), String.valueOf(fragmentIndex++));
        processSession.transfer(processSession.putAllAttributes(fragment, attributes), relSplit);
    }

    private void write(Event e, SplitLargeJson.JsonParserView view) {
        switch (e) {
            case START_ARRAY:
                generator.writeStartArray();
                break;
            case START_OBJECT:
                generator.writeStartObject();
                break;
            case KEY_NAME:
                generator.writeKey(view.getString());
                break;
            case VALUE_STRING:
                generator.write(view.getString());
                break;
            case VALUE_NUMBER:
                if (view.isIntegralNumber()) {
                    generator.write(view.getInt());
                } else {
                    generator.write(view.getBigDecimal().doubleValue());
                }
                break;
            case VALUE_TRUE:
                generator.write(true);
                break;
            case VALUE_FALSE:
                generator.write(false);
                break;
            case VALUE_NULL:
                generator.writeNull();
                break;
            case END_OBJECT:
            case END_ARRAY:
                generator.writeEnd();
                break;
        }
    }

    public void filterEvent(BoolPair section, Event e, SplitLargeJson.JsonParserView view, int stackDepth) throws IOException {
        switch (mode) {
            case CONTIGUOUS:
                filterContiguous(section, e, view, stackDepth);
                break;
            case DISJOINT:
                filterDisjoint(section, e, view);
                break;
            default:
                throw new IllegalStateException();
        }
    }

    private void filterDisjoint(BoolPair section, Event e, SplitLargeJson.JsonParserView view) throws IOException {
        if (section.waxing()) {
            if (e == Event.KEY_NAME) {
                context = Event.START_OBJECT;
                contextName = view.getString();
            } else {
                context = Event.START_ARRAY;
                contextName = null;
            }
        } else if (section.full()) {
            if (generator == null) {
                newFragment(view);
            }
            write(e, view);
        } else if (section.waning()) {
            if (generator == null) {
                writeDisjointSingle(e, view);
            } else {
                write(e, view);
                endFragment();
            }
        }
    }

    private void writeDisjointSingle(Event e, SplitLargeJson.JsonParserView view) throws IOException {
        addContext = true;
        newFragment(view);
        if (contextName != null) {
            generator.writeKey(contextName);
        }
        write(e, view);
        endFragment();
        addContext = false;
    }

    private void filterContiguous(BoolPair section, Event e, SplitLargeJson.JsonParserView view, int stackDepth)
            throws IOException {
        if (section.full()) {
            if (context == null) {
                targetDepth = stackDepth;
                if ((e != Event.START_ARRAY) && (e != Event.START_OBJECT)) {
                    throw new IllegalArgumentException("First event must be the beginning of an array or object");
                }
                context = e;
            } else {
                if (addContext == null) {
                    addContext = (e != Event.START_ARRAY && e != Event.START_OBJECT);
                }
                if (generator == null) {
                    newFragment(view);
                }
                write(e, view);
                if ((addContext && context != Event.START_OBJECT) ||
                    ((stackDepth == targetDepth) && (stackDepth < prevDepth))) {
                    endFragment();
                }
            }
        }
        prevDepth = stackDepth;
    }
}
