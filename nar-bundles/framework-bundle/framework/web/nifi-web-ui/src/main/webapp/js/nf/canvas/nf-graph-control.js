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
nf.GraphControl = (function () {

    var config = {
        translateIncrement: 20
    };

    return {
        /**
         * Initializes the graph controls.
         */
        init: function () {
            // pan up
            nf.Common.addHoverEffect('#pan-up-button', 'pan-up', 'pan-up-hover').click(function () {
                var translate = nf.Canvas.View.translate();
                nf.Canvas.View.translate([translate[0], translate[1] + config.translateIncrement]);

                // refresh the canvas
                nf.Canvas.View.refresh({
                    transition: true
                });
            });

            // pan down
            nf.Common.addHoverEffect('#pan-down-button', 'pan-down', 'pan-down-hover').click(function () {
                var translate = nf.Canvas.View.translate();
                nf.Canvas.View.translate([translate[0], translate[1] - config.translateIncrement]);

                // refresh the canvas
                nf.Canvas.View.refresh({
                    transition: true
                });
            });

            // pan left
            nf.Common.addHoverEffect('#pan-left-button', 'pan-left', 'pan-left-hover').click(function () {
                var translate = nf.Canvas.View.translate();
                nf.Canvas.View.translate([translate[0] + config.translateIncrement, translate[1]]);

                // refresh the canvas
                nf.Canvas.View.refresh({
                    transition: true
                });
            });

            // pan right
            nf.Common.addHoverEffect('#pan-right-button', 'pan-right', 'pan-right-hover').click(function () {
                var translate = nf.Canvas.View.translate();
                nf.Canvas.View.translate([translate[0] - config.translateIncrement, translate[1]]);

                // refresh the canvas
                nf.Canvas.View.refresh({
                    transition: true
                });
            });

            // zoom in
            nf.Common.addHoverEffect('#zoom-in-button', 'zoom-in', 'zoom-in-hover').click(function () {
                nf.Canvas.View.zoomIn();

                // refresh the canvas
                nf.Canvas.View.refresh({
                    transition: true
                });
            });

            // zoom out
            nf.Common.addHoverEffect('#zoom-out-button', 'zoom-out', 'zoom-out-hover').click(function () {
                nf.Canvas.View.zoomOut();

                // refresh the canvas
                nf.Canvas.View.refresh({
                    transition: true
                });
            });

            // zoom fit
            nf.Common.addHoverEffect('#zoom-fit-button', 'fit-image', 'fit-image-hover').click(function () {
                nf.Canvas.View.fit();

                // refresh the canvas
                nf.Canvas.View.refresh({
                    transition: true
                });
            });

            // one to one
            nf.Common.addHoverEffect('#zoom-actual-button', 'actual-size', 'actual-size-hover').click(function () {
                nf.Canvas.View.actualSize();

                // refresh the canvas
                nf.Canvas.View.refresh({
                    transition: true
                });
            });
        }
    };
}());