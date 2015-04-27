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
package org.apache.nifi.util.search;

import java.io.IOException;
import java.io.InputStream;
import java.util.Set;

import org.apache.nifi.util.search.ahocorasick.SearchState;

/**
 * Defines an interface to search for content given a set of search terms. Any
 * implementation of search must be thread safe.
 *
 */
public interface Search<T> {

    /**
     * Establishes the dictionary of terms which will be searched in subsequent
     * search calls. This can be called only once
     *
     * @param terms the terms to create a dictionary of
     */
    void initializeDictionary(Set<SearchTerm<T>> terms);

    /**
     * Searches the given input stream for matches between the already specified
     * dictionary and the contents scanned.
     *
     * @param haystack the source data to scan for hits
     * @param findAll if true will find all matches if false will find only the
     * first match
     * @return SearchState containing results Map might be empty which indicates
     * no matches found but will not be null
     * @throws IOException Thrown for any exceptions occurring while searching.
     * @throws IllegalStateException if the dictionary has not yet been
     * initialized
     */
    SearchState<T> search(InputStream haystack, boolean findAll) throws IOException;

}
