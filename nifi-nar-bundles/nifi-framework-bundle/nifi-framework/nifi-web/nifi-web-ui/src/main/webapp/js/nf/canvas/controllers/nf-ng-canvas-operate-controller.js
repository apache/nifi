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

/* global nf, d3 */

nf.ng.Canvas.OperateCtrl = function () {
    'use strict';

    // updates the color if its a valid hex color string
    var updateColor = function () {
        var hex = $('#fill-color-value').val();

        // only update the fill color when its a valid hex color string
        // #[six hex characters|three hex characters] case insensitive
        if (/(^#[0-9A-F]{6}$)|(^#[0-9A-F]{3}$)/i.test(hex)) {
            $('#fill-color').minicolors('value', hex);
        }
    };

    function OperateCtrl() {

        /**
         * The canvas operator's create template component.
         */
        this.template = {

            /**
             * The canvas operator's create template component's modal.
             */
            modal: {

                /**
                 * Gets the modal element.
                 *
                 * @returns {*|jQuery|HTMLElement}
                 */
                getElement: function () {
                    return $('#new-template-dialog');
                },

                /**
                 * Initialize the modal.
                 */
                init: function () {
                    // configure the create template dialog
                    this.getElement().modal({
                        scrollableContentStyle: 'scrollable',
                        headerText: 'Create Template'
                    });
                },

                /**
                 * Updates the modal config.
                 *
                 * @param {string} name             The name of the property to update.
                 * @param {object|array} config     The config for the `name`.
                 */
                update: function (name, config) {
                    this.getElement().modal(name, config);
                },

                /**
                 * Show the modal.
                 */
                show: function () {
                    this.getElement().modal('show');
                },

                /**
                 * Hide the modal.
                 */
                hide: function () {
                    this.getElement().modal('hide');
                }
            }
        };

        /**
         * The canvas operator's create template component.
         */
        this.templateUpload = {

            /**
             * The canvas operator's create template component's modal.
             */
            modal: {

                /**
                 * Gets the modal element.
                 *
                 * @returns {*|jQuery|HTMLElement}
                 */
                getElement: function () {
                    return $('#upload-template-dialog');
                },

                /**
                 * Initialize the modal.
                 */
                init: function () {
                    // initialize the form
                    var templateForm = $('#template-upload-form').ajaxForm({
                        url: '../nifi-api/process-groups/' + encodeURIComponent(nf.Canvas.getGroupId()) + '/templates/upload',
                        dataType: 'xml',
                        success: function (response, statusText, xhr, form) {
                            // see if the import was successful
                            if (response.documentElement.tagName === 'templateEntity') {
                                // close the dialog
                                $('#upload-template-dialog').modal('hide');

                                // close the settings dialog
                                nf.Dialog.showOkDialog({
                                    headerText: 'Success',
                                    dialogContent: 'Template successfully imported.'
                                });
                            } else {
                                // import failed
                                var status = 'Unable to import template. Please check the log for errors.';
                                if (response.documentElement.tagName === 'errorResponse') {
                                    // if a more specific error was given, use it
                                    var errorMessage = response.documentElement.getAttribute('statusText');
                                    if (!nf.Common.isBlank(errorMessage)) {
                                        status = errorMessage;
                                    }
                                }
                                $('#upload-template-status').text(status);
                            }
                        },
                        error: function (xhr, statusText, error) {
                            $('#upload-template-status').text(xhr.responseText);
                        }
                    });
                    
                    // configure the upload template dialog
                    this.getElement().modal({
                        headerText: 'Upload Template',
                        buttons: [{
                            buttonText: 'Upload',
                            color: {
                                base: '#728E9B',
                                hover: '#004849',
                                text: '#ffffff'
                            },
                            handler: {
                                click: function () {
                                    var selectedTemplate = $('#selected-template-name').text();

                                    // submit the template if necessary
                                    if (nf.Common.isBlank(selectedTemplate)) {
                                        $('#upload-template-status').text('No template selected. Please browse to select a template.');
                                    } else {
                                        templateForm.submit();
                                    }
                                }
                            }
                        }, {
                            buttonText: 'Cancel',
                            color: {
                                base: '#E3E8EB',
                                hover: '#C7D2D7',
                                text: '#004849'
                            },
                            handler: {
                                click: function () {
                                    // hide the dialog
                                    $('#upload-template-dialog').modal('hide');
                                }
                            }
                        }],
                        handler: {
                            close: function () {
                                // set the filename
                                $('#selected-template-name').text('');
                                $('#upload-template-status').text('');

                                // reset the form to ensure that the change fire will fire
                                templateForm.resetForm();
                            }
                        }
                    });

                    // add a handler for the change file input chain event
                    $('#template-file-field').on('change', function (e) {
                        var filename = $(this).val();
                        if (!nf.Common.isBlank(filename)) {
                            filename = filename.replace(/^.*[\\\/]/, '');
                        }

                        // set the filename and clear any status
                        $('#selected-template-name').text(filename);
                        $('#upload-template-status').text('');
                    });
                },

                /**
                 * Updates the modal config.
                 *
                 * @param {string} name             The name of the property to update.
                 * @param {object|array} config     The config for the `name`.
                 */
                update: function (name, config) {
                    this.getElement().modal(name, config);
                },

                /**
                 * Show the modal.
                 */
                show: function () {
                    this.getElement().modal('show');
                },

                /**
                 * Hide the modal.
                 */
                hide: function () {
                    this.getElement().modal('hide');
                }
            }
        };

        /**
         * The canvas operator's fillcolor component.
         */
        this.fillcolor = {

            /**
             * The canvas operator's fillcolor component's modal.
             */
            modal: {

                /**
                 * Gets the modal element.
                 *
                 * @returns {*|jQuery|HTMLElement}
                 */
                getElement: function () {
                    return $('#fill-color-dialog');
                },

                /**
                 * Initialize the modal.
                 */
                init: function () {
                    // configure the create fillcolor dialog
                    this.getElement().modal({
                        scrollableContentStyle: 'scrollable',
                        headerText: 'Change Color',
                        buttons: [{
                            buttonText: 'Apply',
                            color: {
                                base: '#728E9B',
                                hover: '#004849',
                                text: '#ffffff'
                            },
                            handler: {
                                click: function () {
                                    var selection = nf.CanvasUtils.getSelection();

                                    // color the selected components
                                    selection.each(function (d) {
                                        var selected = d3.select(this);
                                        var selectedData = selected.datum();

                                        // get the color and update the styles
                                        var color = $('#fill-color').minicolors('value');

                                        // ensure the color actually changed
                                        if (color !== selectedData.component.style['background-color']) {
                                            // build the request entity
                                            var entity = {
                                                'revision': nf.Client.getRevision(selectedData),
                                                'component': {
                                                    'id': selectedData.id,
                                                    'style': {
                                                        'background-color': color
                                                    }
                                                }
                                            };

                                            // update the style for the specified component
                                            $.ajax({
                                                type: 'PUT',
                                                url: selectedData.uri,
                                                data: JSON.stringify(entity),
                                                dataType: 'json',
                                                contentType: 'application/json'
                                            }).done(function (response) {
                                                // update the component
                                                nf[selectedData.type].set(response);
                                            }).fail(function (xhr, status, error) {
                                                if (xhr.status === 400 || xhr.status === 404 || xhr.status === 409) {
                                                    nf.Dialog.showOkDialog({
                                                        headerText: 'Error',
                                                        dialogContent: nf.Common.escapeHtml(xhr.responseText)
                                                    });
                                                }
                                            }).always(function(){
                                                nf.Birdseye.refresh();
                                            });
                                        }
                                    });

                                    // close the dialog
                                    $('#fill-color-dialog').modal('hide');
                                }
                            }
                        },
                            {
                                buttonText: 'Cancel',
                                color: {
                                    base: '#E3E8EB',
                                    hover: '#C7D2D7',
                                    text: '#004849'
                                },
                                handler: {
                                    click: function () {
                                        // close the dialog
                                        $('#fill-color-dialog').modal('hide');
                                    }
                                }
                            }],
                        handler: {
                            close: function () {
                                // clear the current color
                                $('#fill-color-value').val('');
                                $('#fill-color').minicolors('value', '');
                            }
                        }
                    });
                },

                /**
                 * Updates the modal config.
                 *
                 * @param {string} name             The name of the property to update.
                 * @param {object|array} config     The config for the `name`.
                 */
                update: function (name, config) {
                    this.getElement().modal(name, config);
                },

                /**
                 * Show the modal.
                 */
                show: function () {
                    this.getElement().modal('show');
                },

                /**
                 * Hide the modal.
                 */
                hide: function () {
                    this.getElement().modal('hide');
                },

                /**
                 * The canvas operator's fillcolor component modal's minicolors.
                 */
                minicolors: {

                    /**
                     * Gets the minicolors element.
                     *
                     * @returns {*|jQuery|HTMLElement}
                     */
                    getElement: function () {
                        return $('#fill-color');
                    },

                    /**
                     * Initialize the minicolors.
                     */
                    init: function () {
                        // configure the minicolors
                        this.getElement().minicolors({
                            inline: true,
                            change: function (hex, opacity) {
                                // update the value
                                $('#fill-color-value').val(hex);

                                // always update the preview
                                $('#fill-color-processor-preview').css('color', hex);
                                $('#fill-color-label-preview').css({
                                    'border-color': hex,
                                    'background': 'linear-gradient(to bottom, #ffffff, ' + hex + ')',
                                    'filter': 'progid:DXImageTransform.Microsoft.gradient(gradientType=0, startColorstr=#ffffff, endColorstr=' + hex + ')'
                                });
                            }
                        });

                        // apply fill color from field on blur and enter press
                        $('#fill-color-value').on('blur', updateColor).on('keyup', function (e) {
                            var code = e.keyCode ? e.keyCode : e.which;
                            if (code === $.ui.keyCode.ENTER) {
                                updateColor();
                            }
                        });
                    }
                }
            }
        };
    }

    OperateCtrl.prototype = {
        constructor: OperateCtrl,

        /**
         * Initializes the canvas operate controller.
         */
        init: function () {
            this.template.modal.init();
            this.templateUpload.modal.init();
            this.fillcolor.modal.init();
            this.fillcolor.modal.minicolors.init();
        }
    }

    var operateCtrl = new OperateCtrl();
    return operateCtrl;
};