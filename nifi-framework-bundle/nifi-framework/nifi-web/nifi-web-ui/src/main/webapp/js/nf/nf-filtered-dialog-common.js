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
            return (nf.FilteredDialogCommon = factory($));
        });
    } else if (typeof exports === 'object' && typeof module === 'object') {
        module.exports = (nf.FilteredDialogCommon = factory(require('jquery')));
    } else {
        nf.FilteredDialogCommon = factory(root.$);
    }
}(this, function ($) {
    'use strict';

    var nfFilteredDialogCommon = {

        /**
         * Registers keydown event on dialog's filter input in order to navigate the grid's rows via keyboard arrow keys.
         *
         * @argument {string}   filter        Filter input box selector
         * @argument {object}   grid          SlickGrid reference
         * @argument {object}   dataview      Current grid dataset
         */
        addKeydownListener: function (filter, grid, dataview) {
            var navigationKeys = [$.ui.keyCode.UP, $.ui.keyCode.PAGE_UP, $.ui.keyCode.DOWN, $.ui.keyCode.PAGE_DOWN];
            
            // setup the navigation
            $(filter).off('keydown').on('keydown', function(e) {
                var code = e.keyCode ? e.keyCode : e.which;

                // nonmodified navigation keys only
                if ($.inArray(code, navigationKeys) === -1 || e.shiftKey || e.altKey || e.ctrlKey) {
                    return;
                }

                var selected = grid.getSelectedRows();

                if (selected.length > 0) {
                    if (code === $.ui.keyCode.PAGE_UP) {
                        grid.navigatePageUp();
                        return;
                    }
                    if (code === $.ui.keyCode.PAGE_DOWN) {
                        grid.navigatePageDown();
                        return;
                    }

                    // grid multi-select = false
                    var nextIndex = selected[0];

                    // get the following/previous row
                    if (code === $.ui.keyCode.UP) {
                        nextIndex = Math.max(nextIndex - 1, 0);
                    } else {
                        nextIndex = Math.min(nextIndex + 1, dataview.getLength() - 1);
                    }

                    nfFilteredDialogCommon.choseRow(grid, nextIndex);
                    // ensure the newly selected row is visible
                    grid.scrollRowIntoView(nextIndex, false);
                }
            });
        },

        /**
         * Selects the first row and activates the first cell within given SlickGrid.
         *
         * @argument {object}   grid    SlickGrid reference
         */
        choseFirstRow: function (grid) {
            nfFilteredDialogCommon.choseRow(grid, 0);
        },

        /**
         * Selects given row and activates the first cell in the row within given SlickGrid.
         *
         * @argument {object}   grid    SlickGrid reference
         * @argument {int}      index   Row index
         */
        choseRow: function (grid, index) {
            grid.setSelectedRows([index]);
            grid.setActiveCell(index, 0);
        }
    };

    return nfFilteredDialogCommon;
}));