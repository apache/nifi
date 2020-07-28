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
package org.apache.nifi.processors.standard;

import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.Closeable;
import java.io.DataInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.ReentrantLock;
import org.apache.nifi.annotation.behavior.EventDriven;
import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.InputRequirement.Requirement;
import org.apache.nifi.annotation.behavior.SideEffectFree;
import org.apache.nifi.annotation.behavior.SupportsBatching;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.ProcessorInitializationContext;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.io.InputStreamCallback;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.util.file.monitor.LastModifiedMonitor;
import org.apache.nifi.util.file.monitor.SynchronousFileWatcher;
import org.apache.nifi.util.search.Search;
import org.apache.nifi.util.search.SearchTerm;
import org.apache.nifi.util.search.ahocorasick.AhoCorasick;
import org.apache.nifi.util.search.ahocorasick.SearchState;

@EventDriven
@SideEffectFree
@SupportsBatching
@InputRequirement(Requirement.INPUT_REQUIRED)
@Tags({"aho-corasick", "scan", "content", "byte sequence", "search", "find", "dictionary"})
@CapabilityDescription("Scans the content of FlowFiles for terms that are found in a user-supplied dictionary. If a term is matched, the UTF-8 "
        + "encoded version of the term will be added to the FlowFile using the 'matching.term' attribute")
@WritesAttribute(attribute = "matching.term", description = "The term that caused the Processor to route the FlowFile to the 'matched' relationship; "
        + "if FlowFile is routed to the 'unmatched' relationship, this attribute is not added")
public class ScanContent extends AbstractProcessor {

    public static final String TEXT_ENCODING = "text";
    public static final String BINARY_ENCODING = "binary";
    public static final String MATCH_ATTRIBUTE_KEY = "matching.term";

    public static final PropertyDescriptor DICTIONARY = new PropertyDescriptor.Builder()
            .name("Dictionary File")
            .description("The filename of the terms dictionary")
            .required(true)
            .addValidator(StandardValidators.FILE_EXISTS_VALIDATOR)
            .build();
    public static final PropertyDescriptor DICTIONARY_ENCODING = new PropertyDescriptor.Builder()
            .name("Dictionary Encoding")
            .description("Indicates how the dictionary is encoded. If 'text', dictionary terms are new-line delimited and UTF-8 encoded; "
                    + "if 'binary', dictionary terms are denoted by a 4-byte integer indicating the term length followed by the term itself")
            .required(true)
            .allowableValues(TEXT_ENCODING, BINARY_ENCODING)
            .defaultValue(TEXT_ENCODING)
            .build();

    public static final Relationship REL_MATCH = new Relationship.Builder()
            .name("matched")
            .description("FlowFiles that match at least one "
                    + "term in the dictionary are routed to this relationship")
            .build();
    public static final Relationship REL_NO_MATCH = new Relationship.Builder()
            .name("unmatched")
            .description("FlowFiles that do not match any "
                    + "term in the dictionary are routed to this relationship")
            .build();

    public static final Charset UTF8 = Charset.forName("UTF-8");

    private final AtomicReference<SynchronousFileWatcher> fileWatcherRef = new AtomicReference<>();
    private final AtomicReference<Search<byte[]>> searchRef = new AtomicReference<>();
    private final ReentrantLock dictionaryUpdateLock = new ReentrantLock();

    private List<PropertyDescriptor> properties;
    private Set<Relationship> relationships;

    @Override
    protected void init(final ProcessorInitializationContext context) {
        final List<PropertyDescriptor> properties = new ArrayList<>();
        properties.add(DICTIONARY);
        properties.add(DICTIONARY_ENCODING);
        this.properties = Collections.unmodifiableList(properties);

        final Set<Relationship> relationships = new HashSet<>();
        relationships.add(REL_MATCH);
        relationships.add(REL_NO_MATCH);
        this.relationships = Collections.unmodifiableSet(relationships);
    }

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return properties;
    }

    @Override
    public Set<Relationship> getRelationships() {
        return relationships;
    }

    @Override
    public void onPropertyModified(final PropertyDescriptor descriptor, final String oldValue, final String newValue) {
        if (descriptor.equals(DICTIONARY)) {
            fileWatcherRef.set(new SynchronousFileWatcher(Paths.get(newValue), new LastModifiedMonitor(), 60000L));
        }
    }

    private boolean reloadDictionary(final ProcessContext context, final boolean force, final ComponentLog logger) throws IOException {
        boolean obtainedLock;
        if (force) {
            dictionaryUpdateLock.lock();
            obtainedLock = true;
        } else {
            obtainedLock = dictionaryUpdateLock.tryLock();
        }

        if (obtainedLock) {
            try {
                final Search<byte[]> search = new AhoCorasick<>();
                final Set<SearchTerm<byte[]>> terms = new HashSet<>();

                final InputStream inStream = Files.newInputStream(Paths.get(context.getProperty(DICTIONARY).getValue()), StandardOpenOption.READ);

                final TermLoader termLoader;
                if (context.getProperty(DICTIONARY_ENCODING).getValue().equalsIgnoreCase(TEXT_ENCODING)) {
                    termLoader = new TextualTermLoader(inStream);
                } else {
                    termLoader = new BinaryTermLoader(inStream);
                }

                try {
                    SearchTerm<byte[]> term;
                    while ((term = termLoader.nextTerm()) != null) {
                        terms.add(term);
                    }

                    search.initializeDictionary(terms);
                    searchRef.set(search);
                    logger.info("Loaded search dictionary from {}", new Object[]{context.getProperty(DICTIONARY).getValue()});
                    return true;
                } finally {
                    termLoader.close();
                }
            } finally {
                dictionaryUpdateLock.unlock();
            }
        } else {
            return false;
        }
    }

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {
        final ComponentLog logger = getLogger();
        final SynchronousFileWatcher fileWatcher = fileWatcherRef.get();
        try {
            if (fileWatcher.checkAndReset()) {
                reloadDictionary(context, true, logger);
            }
        } catch (final IOException e) {
            throw new ProcessException(e);
        }

        Search<byte[]> search = searchRef.get();
        try {
            if (search == null) {
                if (reloadDictionary(context, false, logger)) {
                    search = searchRef.get();
                }
            }
        } catch (final IOException e) {
            throw new ProcessException(e);
        }

        if (search == null) {
            return;
        }

        FlowFile flowFile = session.get();
        if (flowFile == null) {
            return;
        }

        final Search<byte[]> finalSearch = search;
        final AtomicReference<SearchTerm<byte[]>> termRef = new AtomicReference<>(null);
        termRef.set(null);

        session.read(flowFile, new InputStreamCallback() {
            @Override
            public void process(final InputStream rawIn) throws IOException {
                try (final InputStream in = new BufferedInputStream(rawIn)) {
                    final SearchState<byte[]> searchResult = finalSearch.search(in, false);
                    if (searchResult.foundMatch()) {
                        termRef.set(searchResult.getResults().keySet().iterator().next());
                    }
                }
            }
        });

        final SearchTerm<byte[]> matchingTerm = termRef.get();
        if (matchingTerm == null) {
            logger.info("Routing {} to 'unmatched'", new Object[]{flowFile});
            session.getProvenanceReporter().route(flowFile, REL_NO_MATCH);
            session.transfer(flowFile, REL_NO_MATCH);
        } else {
            final String matchingTermString = matchingTerm.toString(UTF8);
            logger.info("Routing {} to 'matched' because it matched term {}", new Object[]{flowFile, matchingTermString});
            flowFile = session.putAttribute(flowFile, MATCH_ATTRIBUTE_KEY, matchingTermString);
            session.getProvenanceReporter().route(flowFile, REL_MATCH);
            session.transfer(flowFile, REL_MATCH);
        }
    }

    private interface TermLoader extends Closeable {

        SearchTerm<byte[]> nextTerm() throws IOException;
    }

    private static class TextualTermLoader implements TermLoader {

        private final BufferedReader reader;

        public TextualTermLoader(final InputStream inStream) {
            this.reader = new BufferedReader(new InputStreamReader(inStream));
        }

        @Override
        public SearchTerm<byte[]> nextTerm() throws IOException {
            final String nextLine = reader.readLine();
            if (nextLine == null || nextLine.isEmpty()) {
                return null;
            }
            return new SearchTerm<>(nextLine.getBytes("UTF-8"));
        }

        @Override
        public void close() throws IOException {
            this.reader.close();
        }
    }

    private static class BinaryTermLoader implements TermLoader {

        private final DataInputStream inStream;

        public BinaryTermLoader(final InputStream inStream) {
            this.inStream = new DataInputStream(new BufferedInputStream(inStream));
        }

        @Override
        public SearchTerm<byte[]> nextTerm() throws IOException {
            inStream.mark(1);
            final int nextByte = inStream.read();
            if (nextByte == -1) {
                return null;
            }

            inStream.reset();
            final int termLength = inStream.readInt();
            final byte[] term = new byte[termLength];
            inStream.readFully(term);

            return new SearchTerm<>(term);
        }

        @Override
        public void close() throws IOException {
            this.inStream.close();
        }
    }
}
