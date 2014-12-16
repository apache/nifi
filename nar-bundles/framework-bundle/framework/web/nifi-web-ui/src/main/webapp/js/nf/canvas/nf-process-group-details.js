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
nf.ProcessGroupDetails = (function () {

    return {
        init: function () {
            // configure the processor details dialog
            $('#process-group-details').modal({
                headerText: 'Process Group Details',
                overlayBackground: true,
                buttons: [{
                        buttonText: 'Ok',
                        handler: {
                            click: function () {
                                // hide the dialog
                                $('#process-group-details').modal('hide');
                            }
                        }
                    }],
                handler: {
                    close: function () {
                        // clear the processor details
                        nf.Common.clearField('read-only-process-group-name');
                        nf.Common.clearField('read-only-process-group-comments');
                    }
                }
            });
        },
        
        showDetails: function (selection) {
            // if the specified selection is a process group
            if (nf.CanvasUtils.isProcessGroup(selection)) {
                var selectionData = selection.datum();

                // populate the port settings
                nf.Common.populateField('read-only-process-group-name', selectionData.component.name);
                nf.Common.populateField('read-only-process-group-comments', selectionData.component.comments);

                // show the details
                $('#process-group-details').modal('show');
            }
        }
    };
}());