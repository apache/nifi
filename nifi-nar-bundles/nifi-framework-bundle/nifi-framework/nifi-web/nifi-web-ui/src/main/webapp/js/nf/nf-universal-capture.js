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

/* global define, module, require, exports */

(function (root, factory) {
    if (typeof define === 'function' && define.amd) {
        define(['jquery'], function ($) {
            return (nf.UniversalCapture = factory($));
        });
    } else if (typeof exports === 'object' && typeof module === 'object') {
        module.exports = (nf.UniversalCapture = factory(require('jquery')));
    } else {
        nf.UniversalCapture = factory(root.$);
    }
}(this, function ($) {
    'use strict';

    /**
     * Captures keydown on the window to ensure certain keystrokes are handled in a consistent manner, particularly those
     * that can lead to browser navigation/reload.
     */
    $(document).ready(function ($) {
        // setup a listener to ensure keystrokes are being overridden in a consistent manner
        $(window).on('keydown', function (evt) {
            // consider escape, before checking dialogs
            var isCtrl = evt.ctrlKey || evt.metaKey;
            if (!isCtrl && evt.keyCode === 27) {
                // esc

                // prevent escape when editing a property with allowable values - that component does not handle key
                // events so it can bubble up to here. once here we are unable to cancel the current edit so we simply
                // return. this is not an issue for viewing in read only mode as the table is not in an edit mode. this
                // is not an issue for other fields as they can handle key events locally and cancel the edit appropriately
                var visibleCombo = $('div.value-combo');
                if (visibleCombo.is(':visible') && visibleCombo.parent().hasClass('combo-editor')) {
                    return;
                }

                // consider property detail dialogs
                if ($('div.property-detail').is(':visible')) {
                    nfUniversalDialog.removeAllPropertyDetailDialogs();

                    // prevent further bubbling as we're already handled it
                    evt.stopImmediatePropagation();
                    evt.preventDefault();
                } else {
                    var target = $(evt.target);
                    if (target.length) {
                        // special handling for body as the target
                        var cancellables = $('.cancellable');
                        if (cancellables.length) {
                            var zIndexMax = null;
                            var dialogMax = null;

                            // identify the top most cancellable
                            $.each(cancellables, function (_, cancellable) {
                                var dialog = $(cancellable);
                                var zIndex = dialog.css('zIndex');

                                // if the dialog has a zIndex consider it
                                if (dialog.is(':visible') && (zIndex !== null && typeof zIndex !== 'undefined')) {
                                    zIndex = parseInt(zIndex, 10);
                                    if (zIndexMax === null || zIndex > zIndexMax) {
                                        zIndexMax = zIndex;
                                        dialogMax = dialog;
                                    }
                                }
                            });

                            // if we've identified a dialog to close do so and stop propagation
                            if (dialogMax !== null) {
                                // hide the cancellable
                                if (dialogMax.hasClass('modal')) {
                                    dialogMax.modal('hide');
                                } else {
                                    dialogMax.hide();
                                }

                                // prevent further bubbling as we're already handled it
                                evt.stopImmediatePropagation();
                                evt.preventDefault();

                                return;
                            }
                        }

                        // now see if we're in a frame
                        if (top !== window) {
                            // and our parent has shell defined
                            if (typeof parent.nf !== 'undefined' && typeof parent.nf.Shell !== 'undefined') {
                                parent.$('#shell-close-button').click();

                                // prevent further bubbling as we're already handled it
                                evt.stopImmediatePropagation();
                                evt.preventDefault();

                                return;
                            }
                        }
                    }
                }
            } else {
                if (isCtrl) {
                    if (evt.keyCode === 82) {
                        // ctrl-r
                        evt.preventDefault();
                    }
                } else {
                    if (!$('input, textarea').is(':focus') && (evt.keyCode == 8 || evt.keyCode === 46)) {
                        // backspace or delete
                        evt.preventDefault();
                    }
                }
            }
        });
    });

    var nfUniversalDialog = {
        /**
         * Removes all read only property detail dialogs.
         */
        removeAllPropertyDetailDialogs: function () {
            var propertyDetails = $('body').find('div.property-detail');
            propertyDetails.find('div.nfel-editor').nfeditor('destroy'); // look for any nfel editors
            propertyDetails.find('div.value-combo').combo('destroy'); // look for any combos
            propertyDetails.hide().remove();
        }
    };

    return nfUniversalDialog;
}));