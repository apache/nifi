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


nf.ng.Canvas.GraphControlsCtrl = (function () {

    function GraphControlsCtrl(serviceProvider, navigateCtrl, operateCtrl) {

        var MIN_GRAPH_CONTROL_TOP = 117;

        /**
         * Positions the graph controls based on the size of the screen.
         */
        var positionGraphControls = function () {
            var windowHeight = $(window).height();
            var navigationHeight = $('#navigation-control').outerHeight();
            var operationHeight = $('#operation-control').outerHeight();
            var graphControlTop = (windowHeight / 2) - ((navigationHeight + operationHeight) / 2);

            $('#graph-controls').css('top', Math.max(MIN_GRAPH_CONTROL_TOP, graphControlTop));
        };

        /**
         * Opens the specified graph control.
         *
         * @param {jQuery} graphControl
         */
        var openGraphControl = function (graphControl) {
            // undock if necessary
            if ($('div.graph-control-content').is(':visible') === false) {
                $('#graph-controls div.graph-control-docked').hide();
                $('#graph-controls div.graph-control-header-container').show();
                $('.graph-control').removeClass('docked');
            }

            // show the content of the specified graph control
            graphControl.children('div.graph-control-content').show();
            graphControl.find('i.graph-control-expansion').removeClass('fa-plus-square-o').addClass('fa-minus-square-o');

            // handle specific actions as necessary
            if (graphControl.attr('id') === 'navigation-control') {
                nf.Birdseye.updateBirdseyeVisibility(true);
            }

            // get the current visibility
            var graphControlVisibility = nf.Storage.getItem('graph-control-visibility');
            if (graphControlVisibility === null) {
                graphControlVisibility = {};
            }

            // update the visibility for this graph control
            var graphControlId = graphControl.attr('id');
            graphControlVisibility[graphControlId] = true;
            nf.Storage.setItem('graph-control-visibility', graphControlVisibility);

            // reset the graph control position
            positionGraphControls();
        };

        /**
         * Hides the specified graph control.
         *
         * @param {jQuery} graphControl
         */
        var hideGraphControl = function (graphControl) {
            // hide the content of the specified graph control
            graphControl.children('div.graph-control-content').hide();
            graphControl.find('i.graph-control-expansion').removeClass('fa-minus-square-o').addClass('fa-plus-square-o');

            // dock if necessary
            if ($('div.graph-control-content').is(':visible') === false) {
                $('#graph-controls div.graph-control-header-container').hide();
                $('#graph-controls div.graph-control-docked').show();
                $('.graph-control').addClass('docked');
            }

            // handle specific actions as necessary
            if (graphControl.attr('id') === 'navigation-control') {
                nf.Birdseye.updateBirdseyeVisibility(false);
            }

            // get the current visibility
            var graphControlVisibility = nf.Storage.getItem('graph-control-visibility');
            if (graphControlVisibility === null) {
                graphControlVisibility = {};
            }

            // update the visibility for this graph control
            var graphControlId = graphControl.attr('id');
            graphControlVisibility[graphControlId] = false;
            nf.Storage.setItem('graph-control-visibility', graphControlVisibility);

            // reset the graph control position
            positionGraphControls();
        };

        function GraphControlsCtrl(navigateCtrl, operateCtrl) {
            this.navigateCtrl = navigateCtrl;
            this.operateCtrl = operateCtrl;
        };
        GraphControlsCtrl.prototype = {
            constructor: GraphControlsCtrl,

            /**
             *  Register the header controller.
             */
            register: function () {
                if (serviceProvider.graphControlsCtrl === undefined) {
                    serviceProvider.register('graphControlsCtrl', graphControlsCtrl);
                }
            },

            /**
             * Initialize the graph controls.
             */
            init: function () {
                this.operateCtrl.init();
                // initial the graph control visibility
                var graphControlVisibility = nf.Storage.getItem('graph-control-visibility');
                if (graphControlVisibility !== null) {
                    $.each(graphControlVisibility, function (id, isVisible) {
                        var graphControl = $('#' + id);
                        if (graphControl) {
                            if (isVisible) {
                                openGraphControl(graphControl);
                            } else {
                                hideGraphControl(graphControl);
                            }
                        }
                    });
                }

                // listen for browser resize events to reset the graph control positioning
                $(window).resize(positionGraphControls);

                // set the initial position
                positionGraphControls();
            },

            /**
             * Undock the graph control.
             * @param {jQuery} $event
             */
            undock: function ($event) {
                openGraphControl($($event.target).parent().parent());
            },

            /**
             * Expand the graph control.
             * @param {jQuery} $event
             */
            expand: function ($event) {
                var icon = $($event.target);
                if (icon.hasClass('fa-plus-square-o')) {
                    openGraphControl(icon.closest('div.graph-control'));
                } else {
                    hideGraphControl(icon.closest('div.graph-control'));
                }
            }
        };
        var graphControlsCtrl = new GraphControlsCtrl(navigateCtrl, operateCtrl);
        graphControlsCtrl.register();
        return graphControlsCtrl;
    }

    GraphControlsCtrl.$inject = ['serviceProvider', 'navigateCtrl', 'operateCtrl'];

    return GraphControlsCtrl;
}());