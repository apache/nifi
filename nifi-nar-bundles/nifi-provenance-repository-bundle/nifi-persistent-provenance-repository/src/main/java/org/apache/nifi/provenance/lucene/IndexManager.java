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

package org.apache.nifi.provenance.lucene;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;

import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.search.IndexSearcher;

public interface IndexManager extends Closeable {
    IndexSearcher borrowIndexSearcher(File indexDir) throws IOException;

    IndexWriter borrowIndexWriter(File indexingDirectory) throws IOException;

    void removeIndex(final File indexDirectory);

    void returnIndexSearcher(File indexDirectory, IndexSearcher searcher);

    void returnIndexWriter(File indexingDirectory, IndexWriter writer);
}