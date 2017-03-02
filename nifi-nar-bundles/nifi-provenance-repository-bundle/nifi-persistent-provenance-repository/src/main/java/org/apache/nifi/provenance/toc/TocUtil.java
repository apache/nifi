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
package org.apache.nifi.provenance.toc;

import java.io.File;

import org.apache.nifi.provenance.lucene.LuceneUtil;

public class TocUtil {

    /**
     * Returns the file that should be used as the Table of Contents for the given Journal File.
     * Note, if no TOC exists for the given Journal File, a File will still be returned but the file
     * will not actually exist.
     *
     * @param journalFile the journal file for which to get the Table of Contents
     * @return the file that represents the Table of Contents for the specified journal file.
     */
    public static File getTocFile(final File journalFile) {
        final File tocDir = new File(journalFile.getParentFile(), "toc");
        final String basename = LuceneUtil.substringBefore(journalFile.getName(), ".prov");
        final File tocFile = new File(tocDir, basename + ".toc");
        return tocFile;
    }

}
