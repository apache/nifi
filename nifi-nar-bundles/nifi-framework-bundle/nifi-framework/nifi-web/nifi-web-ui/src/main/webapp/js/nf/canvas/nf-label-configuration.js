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

/* global nf */

nf.LabelConfiguration = (function () {

    var labelId = '';

    return {
        /**
         * Initializes the label details dialog.
         */
        init: function () {
            // make the new property dialog draggable
            $('#label-configuration').modal({
                overlayBackground: true,
                buttons: [{
                    buttonText: 'Apply',
                    handler: {
                        click: function () {
                            // get the label data
                            var labelData = d3.select('#id-' + labelId).datum();

                            // get the new values
                            var labelValue = $('#label-value').val();
                            var fontSize = $('#label-font-size').combo('getSelectedOption');

                            // build the label entity
                            var labelEntity = {
                                'revision': nf.Client.getRevision(),
                                'component': {
                                    'id': labelId,
                                    'label': labelValue,
                                    'style': {
                                        'font-size': fontSize.value
                                    }
                                }
                            };

                            // save the new label value
                            $.ajax({
                                type: 'PUT',
                                url: labelData.component.uri,
                                data: JSON.stringify(labelEntity),
                                dataType: 'json',
                                contentType: 'application/json'
                            }).done(function (response) {
                                // update the revision
                                nf.Client.setRevision(response.revision);

                                // get the label out of the response
                                nf.Label.set(response);
                            }).fail(nf.Common.handleAjaxError);

                            // reset and hide the dialog
                            this.modal('hide');
                        }
                    }
                }, {
                    buttonText: 'Cancel',
                    handler: {
                        click: function () {
                            this.modal('hide');
                        }
                    }
                }],
                handler: {
                    close: function () {
                        labelId = '';
                    }
                }
            }).draggable({
                containment: 'parent',
                cancel: 'textarea, .button, .combo'
            });

            // create the available sizes
            var sizes = [];
            for (var i = 12; i <= 24; i += 2) {
                sizes.push({
                    text: i + 'px',
                    value: i + 'px'
                });
            }

            // initialize the font size combo
            $('#label-font-size').combo({
                options: sizes,
                selectedOption: {
                    value: '12px'
                },
                select: function (option) {
                    var labelValue = $('#label-value');

                    // reset the value to trigger IE to update, otherwise the
                    // new line height wasn't being picked up
                    labelValue.css({
                        'font-size': option.value,
                        'line-height': option.value
                    }).val(labelValue.val());
                }
            });
        },
        
        /**
         * Shows the configuration for the specified label.
         * 
         * @argument {selection} selection      The selection
         */
        showConfiguration: function (selection) {
            if (nf.CanvasUtils.isLabel(selection)) {
                var selectionData = selection.datum();

                // get the label value
                var labelValue = '';
                if (nf.Common.isDefinedAndNotNull(selectionData.component.label)) {
                    labelValue = selectionData.component.label;
                }

                // get the font size
                var fontSize = '12px';
                if (nf.Common.isDefinedAndNotNull(selectionData.component.style['font-size'])) {
                    fontSize = selectionData.component.style['font-size'];
                }

                // store the label uri
                labelId = selectionData.id;

                // populate the dialog
                $('#label-value').val(labelValue);
                $('#label-font-size').combo('setSelectedOption', {
                    value: fontSize
                });

                // show the detail dialog
                $('#label-configuration').modal('show');
            }
        }
    };
}());