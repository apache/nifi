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

import { NgModule } from '@angular/core';
import { CommonModule } from '@angular/common';
import { RouterModule, Routes } from '@angular/router';
import { StoreModule } from '@ngrx/store';
import { EffectsModule } from '@ngrx/effects';
import { ConnectorCanvasComponent } from './connector-canvas.component';
import { ConnectorCanvasRedirector } from './guard/connector-canvas-redirector.component';
import { connectorCanvasRootGuard } from './guard/connector-canvas-root.guard';
import { connectorCanvasFeatureKey } from '../../state/connector-canvas';
import { connectorCanvasReducer } from '../../state/connector-canvas/connector-canvas.reducer';
import { ConnectorCanvasEffects } from '../../state/connector-canvas/connector-canvas.effects';
import { ConnectorCanvasEntityEffects } from '../../state/connector-canvas-entity/connector-canvas-entity.effects';

const routes: Routes = [
    {
        path: '',
        component: ConnectorCanvasRedirector,
        canActivate: [connectorCanvasRootGuard],
        pathMatch: 'full'
    },
    {
        path: ':processGroupId',
        component: ConnectorCanvasComponent,
        children: [
            { path: 'bulk/:ids', component: ConnectorCanvasComponent },
            { path: ':type/:componentId', component: ConnectorCanvasComponent }
        ]
    }
];

@NgModule({
    imports: [
        CommonModule,
        ConnectorCanvasComponent,
        ConnectorCanvasRedirector,
        RouterModule.forChild(routes),
        StoreModule.forFeature(connectorCanvasFeatureKey, connectorCanvasReducer),
        EffectsModule.forFeature(ConnectorCanvasEffects, ConnectorCanvasEntityEffects)
    ]
})
export class ConnectorCanvasModule {}
