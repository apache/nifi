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
nf.Selectable = (function () {

    return {
        init: function () {

        },
        select: function (g) {
            // hide any context menus as necessary
            nf.ContextMenu.hide();

            // only need to update selection if necessary
            if (!g.classed('selected')) {
                // since we're not appending, deselect everything else
                if (!d3.event.shiftKey) {
                    d3.selectAll('g.selected').classed('selected', false);
                }

                // update the selection
                g.classed('selected', true);
            } else {
                // we are currently selected, if shift key the deselect
                if (d3.event.shiftKey) {
                    g.classed('selected', false);
                }
            }

            // update the toolbar
            nf.CanvasToolbar.refresh();

            // stop propagation
            d3.event.stopPropagation();
        },
        activate: function (components) {
            components.on('mousedown.selection', function () {
                // get the clicked component to update selection
                nf.Selectable.select(d3.select(this));
            });
        }
    };
}());