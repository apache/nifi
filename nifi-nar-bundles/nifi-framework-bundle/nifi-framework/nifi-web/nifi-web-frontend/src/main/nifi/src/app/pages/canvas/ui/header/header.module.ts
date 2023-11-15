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
import { CommonModule, NgOptimizedImage } from '@angular/common';
import { HeaderComponent } from './header.component';
import { FlowStatus } from './flow-status/flow-status.component';
import { CdkDrag } from '@angular/cdk/drag-drop';
import { NewCanvasItemComponent } from './new-canvas-item/new-canvas-item.component';
import { MatButtonModule } from '@angular/material/button';
import { MatMenuModule } from '@angular/material/menu';
import { MatDividerModule } from '@angular/material/divider';
import { Search } from './search/search.component';
import { FormsModule, ReactiveFormsModule } from '@angular/forms';
import { MatFormFieldModule } from '@angular/material/form-field';
import { MatAutocompleteModule } from '@angular/material/autocomplete';
import { CdkConnectedOverlay, CdkOverlayOrigin } from '@angular/cdk/overlay';
import { RouterLink } from '@angular/router';
import { NifiTooltipDirective } from '../../../../ui/common/tooltips/nifi-tooltip.directive';

@NgModule({
    declarations: [HeaderComponent, NewCanvasItemComponent, FlowStatus, Search],
    exports: [HeaderComponent],
    imports: [
        CommonModule,
        NgOptimizedImage,
        CdkDrag,
        MatButtonModule,
        MatMenuModule,
        MatDividerModule,
        FormsModule,
        ReactiveFormsModule,
        MatFormFieldModule,
        MatAutocompleteModule,
        CdkConnectedOverlay,
        CdkOverlayOrigin,
        RouterLink,
        NifiTooltipDirective
    ]
})
export class HeaderModule {}
