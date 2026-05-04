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

import { Component, input, output } from '@angular/core';
import { ConnectorEntity } from '@nifi/shared';
import { CanvasNavigationControl } from '../../../../../ui/common/navigation-control/canvas-navigation-control.component';
import { BirdseyeComponentData, BirdseyeTransform } from '../../../../../ui/common/birdseye/birdseye.types';
import { Dimension, Position } from '../../../../../ui/common/canvas/canvas.types';
import { ConnectorInfoControl } from './connector-info-control/connector-info-control.component';

@Component({
    selector: 'connector-graph-controls',
    standalone: true,
    imports: [CanvasNavigationControl, ConnectorInfoControl],
    templateUrl: './connector-graph-controls.component.html',
    styleUrls: ['./connector-graph-controls.component.scss']
})
export class ConnectorGraphControls {
    connectorEntity = input<ConnectorEntity | null>(null);
    entitySaving = input<boolean>(false);

    birdseyeComponents = input<BirdseyeComponentData[]>([]);
    birdseyeTransform = input<BirdseyeTransform>({ translate: { x: 0, y: 0 }, scale: 1 });
    canvasDimensions = input<Dimension>({ width: 0, height: 0 });
    canNavigateToParent = input<boolean>(false);

    viewportChange = output<Position>();
    birdseyeDragStart = output<void>();
    birdseyeDragEnd = output<void>();

    zoomIn = output<void>();
    zoomOut = output<void>();
    zoomFit = output<void>();
    zoomActual = output<void>();
    leaveGroup = output<void>();
}
