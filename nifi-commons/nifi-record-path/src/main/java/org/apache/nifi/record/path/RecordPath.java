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

package org.apache.nifi.record.path;

import static org.apache.nifi.record.path.RecordPathParser.PATH;
import static org.apache.nifi.record.path.RecordPathParser.CHILD_REFERENCE;

import org.antlr.runtime.ANTLRStringStream;
import org.antlr.runtime.CharStream;
import org.antlr.runtime.CommonTokenStream;
import org.antlr.runtime.tree.Tree;
import org.apache.nifi.record.path.exception.RecordPathException;
import org.apache.nifi.record.path.paths.RecordPathCompiler;
import org.apache.nifi.record.path.paths.RecordPathSegment;
import org.apache.nifi.record.path.paths.RootPath;
import org.apache.nifi.serialization.record.Record;

public interface RecordPath {

    /**
     * Returns the textual representation of the RecordPath
     *
     * @return the textual representation of the RecordPath
     */
    String getPath();

    /**
     * Evaluates the RecordPath against the given Record, returning a RecordPathResult that contains
     * a FieldValue for each field that matches
     *
     * @param record the Record to evaluate
     * @return a RecordPathResult that contains a FieldValue for each field that matches
     */
    RecordPathResult evaluate(Record record);


    /**
     * Evaluates a RecordPath against the given context node. This allows a RecordPath to be evaluated
     * against a Record via {@link #evaluate(Record)} and then have a Relative RecordPath evaluated against
     * the results. This method will throw an Exception if this RecordPath is an Absolute RecordPath.
     *
     * @param record the Record to evaluate
     * @param contextNode the context node that represents where in the Record the 'current node' or 'context node' is
     * @return a RecordPathResult that contains a FieldValue for each field that matches
     */
    RecordPathResult evaluate(Record record, FieldValue contextNode);

    /**
     * Indicates whether the RecordPath is an Absolute Path (starts with a '/' character) or a Relative Path (starts with a '.' character).
     *
     * @return <code>true</code> if the RecordPath is an Absolute Path, <code>false</code> if the RecordPath is a Relative Path
     */
    boolean isAbsolute();

    /**
     * Compiles a RecordPath from the given text
     *
     * @param path the textual representation of the RecordPath
     * @return the compiled RecordPath
     * @throws RecordPathException if the given text is not a valid RecordPath
     */
    public static RecordPath compile(final String path) throws RecordPathException {
        try {
            final CharStream input = new ANTLRStringStream(path);
            final RecordPathLexer lexer = new RecordPathLexer(input);
            final CommonTokenStream lexerTokenStream = new CommonTokenStream(lexer);

            final RecordPathParser parser = new RecordPathParser(lexerTokenStream);
            final Tree tree = (Tree) parser.pathExpression().getTree();

            // We look at the first child, because 'tree' is a PATH_EXPRESSION and we really
            // want the underlying PATH token.
            final Tree firstChild = tree.getChild(0);

            final int childType = firstChild.getType();
            final boolean absolute;
            final RecordPathSegment rootPath;
            if (childType == PATH || childType == CHILD_REFERENCE) {
                rootPath = new RootPath();
                absolute = true;
            } else {
                rootPath = null;
                absolute = false;
            }

            return RecordPathCompiler.compile(firstChild, rootPath, absolute);
        } catch (final RecordPathException e) {
            throw e;
        } catch (final Exception e) {
            throw new RecordPathException(e);
        }
    }

}
