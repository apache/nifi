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
nf.RemoteProcessGroupDetails = (function () {
    return {
        init: function () {
            $('#remote-process-group-details').modal({
                headerText: 'Remote Process Group Details',
                overlayBackground: true,
                buttons: [{
                        buttonText: 'Ok',
                        handler: {
                            click: function () {
                                $('#remote-process-group-details').modal('hide');
                            }
                        }
                    }],
                handler: {
                    close: function () {
                        // clear the remote process group details
                        $('#read-only-remote-process-group-name').text('');
                        $('#read-only-remote-process-group-url').text('');
                        $('#read-only-remote-process-group-timeout').val('');
                        $('#read-only-remote-process-group-yield-duration').val('');
                    }
                }
            }).draggable({
                containment: 'parent',
                handle: '.dialog-header'
            });
        },
        
        /**
         * Shows the details for the remote process group in the specified selection.
         * 
         * @argument {selection} selection      The selection
         */
        showDetails: function (selection) {
            // if the specified component is a remote process group, load its properties
            if (nf.CanvasUtils.isRemoteProcessGroup(selection)) {
                var selectionData = selection.datum();

                // populate the port settings
                $('#read-only-remote-process-group-name').text(selectionData.component.name);
                $('#read-only-remote-process-group-url').text(selectionData.component.targetUri);
                $('#read-only-remote-process-group-timeout').text(selectionData.component.communicationsTimeout);
                $('#read-only-remote-process-group-yield-duration').text(selectionData.component.yieldDuration);

                // show the details
                $('#remote-process-group-details').modal('show');
            }
        }
    };
}());