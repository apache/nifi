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
package org.apache.nifi.toolkit.cli.impl.util;

import org.jline.builtins.Completers;
import org.jline.builtins.Styles;
import org.jline.reader.Candidate;
import org.jline.reader.LineReader;
import org.jline.reader.ParsedLine;
import org.jline.utils.StyleResolver;

import java.io.IOException;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

/**
 * Standard File Name Completer overriding references to Styles.lsStyle() to avoid parsing issues with LS_COLORS
 */
public class StandardFileNameCompleter extends Completers.FileNameCompleter {
    private static final String STANDARD_LS_COLORS = "di=1;91:ex=1;92:ln=1;96:fi=";

    private static final String HOME_DIRECTORY_ALIAS = "~";

    private static final String EMPTY = "";

    private static final StyleResolver STYLE_RESOLVER = Styles.style(STANDARD_LS_COLORS);

    /**
     * Complete file names based on JLine 3.22.0 without calling Styles.lsStyle()
     *
     * @param reader        Line Reader
     * @param commandLine   Parsed Command
     * @param candidates    Candidates to be populated
     */
    @Override
    public void complete(final LineReader reader, final ParsedLine commandLine, final List<Candidate> candidates) {
        assert commandLine != null;
        assert candidates != null;

        final String buffer = commandLine.word().substring(0, commandLine.wordCursor());

        final Path current;
        final String curBuf;
        final String sep = getSeparator(reader.isSet(LineReader.Option.USE_FORWARD_SLASH));
        final int lastSep = buffer.lastIndexOf(sep);
        try {
            if (lastSep >= 0) {
                curBuf = buffer.substring(0, lastSep + 1);
                if (curBuf.startsWith(HOME_DIRECTORY_ALIAS)) {
                    if (curBuf.startsWith(HOME_DIRECTORY_ALIAS + sep)) {
                        current = getUserHome().resolve(curBuf.substring(2));
                    } else {
                        current = getUserHome().getParent().resolve(curBuf.substring(1));
                    }
                } else {
                    current = getUserDir().resolve(curBuf);
                }
            } else {
                curBuf = EMPTY;
                current = getUserDir();
            }

            try (final DirectoryStream<Path> directory = Files.newDirectoryStream(current, this::accept)) {
                directory.forEach(path -> {
                    final String value = curBuf + path.getFileName().toString();
                    if (Files.isDirectory(path)) {
                        candidates.add(
                                new Candidate(
                                        value + (reader.isSet(LineReader.Option.AUTO_PARAM_SLASH) ? sep : EMPTY),
                                        getDisplay(reader.getTerminal(), path, STYLE_RESOLVER, sep),
                                        null,
                                        null,
                                        reader.isSet(LineReader.Option.AUTO_REMOVE_SLASH) ? sep : null,
                                        null,
                                        false
                                )
                        );
                    } else {
                        candidates.add(
                                new Candidate(
                                        value,
                                        getDisplay(reader.getTerminal(), path, STYLE_RESOLVER, sep),
                                        null,
                                        null,
                                        null,
                                        null,
                                        true
                                )
                        );
                    }
                });
            } catch (final IOException e) {
                // Ignore
            }
        } catch (final Exception e) {
            // Ignore
        }
    }
}
