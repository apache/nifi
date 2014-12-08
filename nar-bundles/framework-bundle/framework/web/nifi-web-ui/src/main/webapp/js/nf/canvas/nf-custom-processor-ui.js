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
nf.CustomProcessorUi = {
    /**
     * Shows the custom ui.
     * 
     * @argument {string} processorId       The processor id
     * @argument {string} uri               The uri for the custom ui
     * @argument {boolean} editable         Whether the custom ui should support editing
     */
    showCustomUi: function (processorId, uri, editable) {

        // record the processor id
        $('#shell-close-button');

        var revision = nf.Client.getRevision();

        // build the customer ui params
        var customUiParams = {
            'processorId': processorId,
            'revision': revision.version,
            'clientId': revision.clientId,
            'editable': editable
        };

        // show the shell
        return nf.Shell.showPage('..' + uri + '?' + $.param(customUiParams), false);
    }
};