/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import { NgModule } from '@angular/core';
import { CommonModule } from '@angular/common';
import { StandardContentViewer } from './standard-content-viewer.component';
import { StandardContentViewerRoutingModule } from './standard-content-viewer-routing.module';
import { MatButtonModule } from '@angular/material/button';
import { MatIconModule } from '@angular/material/icon';
import { MatProgressSpinnerModule } from '@angular/material/progress-spinner';
import { ReactiveFormsModule } from '@angular/forms';
import { MatButtonToggleModule } from '@angular/material/button-toggle';
import { NgxSkeletonLoaderModule } from 'ngx-skeleton-loader';
import { Codemirror } from '@nifi/shared';

@NgModule({
    declarations: [StandardContentViewer],
    exports: [StandardContentViewer],
    imports: [
        CommonModule,
        StandardContentViewerRoutingModule,
        MatButtonModule,
        MatIconModule,
        MatProgressSpinnerModule,
        ReactiveFormsModule,
        MatButtonToggleModule,
        NgxSkeletonLoaderModule,
        Codemirror
    ]
})
export class StandardContentViewerModule {}
