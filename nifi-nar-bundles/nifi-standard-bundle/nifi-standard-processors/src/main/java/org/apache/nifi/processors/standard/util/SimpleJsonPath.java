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

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

/** A <code>SimpleJsonPath</code> instance serves as a functional representation of a JSON Path string. An instance
 * of this class can be used to determine whether a particular location in a JSON document is targeted by a particular
 * JSON Path expression. It is  a list of the fundamental elements of a JSON Path, as designated by its inner classes
 * (<code>Element</code> and its subclasses). */
public class SimpleJsonPath {
    private final List<Element> path;
    private final List<Element> altPath;
    private Boolean hasWildcard;

    private SimpleJsonPath(List<Element> path) {
        this.path = Collections.unmodifiableList(path);
        this.altPath = null;
    }

    private SimpleJsonPath(List<Element> path, List<Element> altPath) {
        this.path = Collections.unmodifiableList(path);
        this.altPath = Collections.unmodifiableList(altPath);
    }

    /* Unfortunately, the "[*]" construct is ambiguous in the JSON Path specification.   It could either
     * match a range of array elements OR a range of object fields.  For example, see the disjointEmbeddedStar
     * unit tests.  This ambiguity ends up muddying the code.  Accounting for a possible "alternative path" (altPath)
     * is probably the cleanest (though maybe not the most efficient) way to handle the oddity. */
    public static SimpleJsonPath of(String str) {
        str = str.replaceAll("\\[\\*\\]$", "").replaceAll("\\.\\*$", "");
        if (str.equals("$")) {
            return new SimpleJsonPath(new ArrayList<>());
        }
        if (!str.startsWith("$") || str.endsWith(".") || !isSupported(str)) {
            throw new IllegalArgumentException("Invalid or unsupported path");
        }
        if (str.contains("[*]")) {
            return new SimpleJsonPath(build(str), build(str.replaceAll("\\[\\*\\]", ".*")));
        } else {
            return new SimpleJsonPath(build(str));
        }
    }

    /* Returns false if unsupported characters are found in the supplied JsonPath string; true otherwise.
     * Calling this method on a valid-but-unsupported JsonPath expression will be faster than attempting construction
     * of an unsupported JsonPath and catching the subsequent exception.  The throw-catch mechanics are expensive,
     * from a timing perspective. */
    public static boolean isSupported(String path) {
        return !path.contains("..") && path.matches("[-\\w.\\[\\]$'*]+");
    }

    private static List<Element> build(String str) {
        List<Element> path = new ArrayList<>();
        str = str.substring(1).replaceAll("\\['", ".").replaceAll("'\\]", "");
        String[] parts = str.split("[\\.\\[]");
        for (String part : parts) {
            if (part.matches("^\\d.*") || part.equals("*]")) {
                String[] subParts = part.split("]");
                for (String subPart : subParts) {
                    if ("*".equals(subPart)) {
                        path.add(new NumberedElement());
                    } else {
                        path.add(new FixedNumberedElement(Integer.parseInt(subPart)));
                    }
                }
            } else if (!part.isEmpty()) {
                path.add(new ObjectElement());
                if ("*".equals(part)) {
                    path.add(new NamedElement());
                } else {
                    path.add(new NamedElement(part));
                }
            }
        }
        return path;
    }

    public boolean isEmpty() {
        return path.isEmpty();
    }

    public boolean isBeginningOf(JsonStack stack) {
        boolean startsWith = stackStartsWith(stack, path);
        if (!startsWith && altPath != null) {
            return stackStartsWith(stack, altPath);
        } else {
            return startsWith;
        }
    }

    private boolean stackStartsWith(JsonStack stack, List<Element> path) {
        if (path.isEmpty()) {
            return !stack.isEmpty();
        }
        Iterator<Element> i = stack.iterator();
        int p = 0;
        while (i.hasNext() && (p < path.size())) {
            if (!i.next().equals(path.get(p))) {
                break;
            }
            p++;
        }
        return (p == path.size());
    }

    public boolean hasWildcard() {
        // this could be determined and set at construction time, but there's a lot going on in the build() method,
        // so the second traversal seems worth the reduced clutter...
        if (hasWildcard != null) {
            return hasWildcard;
        } else {
            hasWildcard = false;
            for (Element e : path) {
                if (e.wildcard) {
                    hasWildcard = true;
                    break;
                }
            }
        }
        return hasWildcard;
    }

    static abstract class Element {
        final boolean wildcard;

        Element(boolean wildcard) {
            this.wildcard = wildcard;
        }
    }

    public static class ObjectElement extends  Element {
        ObjectElement() {
            super(false);
        }
        @Override
        public boolean equals(Object o) {
            return ((this == o) || ((o != null) && (getClass() == o.getClass())));
        }
    }

    public static class NamedElement extends Element {
        private final String name;

        NamedElement() {
            super(true);
            name = null;
        }

        NamedElement(String name) {
            super(false);
            this.name = name;
        }

        String getName() {
            return name;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            NamedElement that = (NamedElement) o;

            return (wildcard || that.wildcard || (name != null ? name.equals(that.name) : that.name == null));
        }

        @Override
        public int hashCode() {
            return name != null ? name.hashCode() : 0;
        }
    }

    public static class NumberedElement extends Element {
        private int index;

        NumberedElement() {
            super(true);
        }

        NumberedElement(int index) {
            super(false);
            this.index = index;
        }

        int getIndex() {
            return index;
        }

        public void increment() {
            index++;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (!(o instanceof NumberedElement)) return false;

            NumberedElement that = (NumberedElement) o;

            return (wildcard || that.wildcard || (index == that.index));
        }

        @Override
        public int hashCode() {
            return index;
        }
    }

    public static final class FixedNumberedElement extends NumberedElement {

        FixedNumberedElement(int index) {
            super(index);
        }

        @Override
        public void increment() {
            throw new RuntimeException("Not allowed");
        }
    }
}
