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

package org.apache.nifi.controller.repository;

import java.io.EOFException;

/**
 * Signals that a Swap File could not be complete read in/parsed because the data was
 * not all present
 */
public class IncompleteSwapFileException extends EOFException {
    private static final long serialVersionUID = -6818558584430076898L;

    private final String swapLocation;
    private final SwapContents partialContents;

    public IncompleteSwapFileException(final String swapLocation, final SwapContents partialContents) {
        super();
        this.swapLocation = swapLocation;
        this.partialContents = partialContents;
    }

    public String getSwapLocation() {
        return swapLocation;
    }

    public SwapContents getPartialContents() {
        return partialContents;
    }
}
