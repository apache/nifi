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
import { MatSidenavModule } from '@angular/material/sidenav';
import { Canvas } from './canvas.component';
import { ContextMenu } from '../../../../ui/common/context-menu/context-menu.component';
import { CdkContextMenuTrigger, CdkMenu, CdkMenuItem, CdkMenuTrigger } from '@angular/cdk/menu';
import { GraphControls } from './graph-controls/graph-controls.component';
import { CanvasRoutingModule } from './canvas-routing.module';
import { HeaderComponent } from './header/header.component';
import { FooterComponent } from './footer/footer.component';
import { FlowAnalysisDrawerComponent } from './header/flow-analysis-drawer/flow-analysis-drawer.component';

@NgModule({
    declarations: [Canvas],
    exports: [Canvas],
    imports: [
        CommonModule,
        CdkMenu,
        CdkMenuItem,
        CdkMenuTrigger,
        CdkContextMenuTrigger,
        CanvasRoutingModule,
        GraphControls,
        ContextMenu,
        HeaderComponent,
        FooterComponent,
        MatSidenavModule,
        FlowAnalysisDrawerComponent
    ]
})
export class CanvasModule {}
