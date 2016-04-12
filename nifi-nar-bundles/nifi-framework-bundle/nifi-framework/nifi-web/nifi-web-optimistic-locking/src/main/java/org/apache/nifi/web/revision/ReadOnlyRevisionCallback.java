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

package org.apache.nifi.web.revision;

import org.apache.nifi.web.Revision;

public interface ReadOnlyRevisionCallback<T> {

    /**
     * Performs some action while a Read Lock is held for the given
     * Revision. Note that this method must never change the component
     * for which the Read Lock is held!
     *
     * @param revision the Revision for which a Read Lock is held
     * @return the result of applying the logic of this callback
     */
    T withRevision(Revision revision);

}
