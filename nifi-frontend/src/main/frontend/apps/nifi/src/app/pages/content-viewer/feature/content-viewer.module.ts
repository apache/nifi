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
import { ContentViewerComponent } from './content-viewer.component';
import { ContentViewerRoutingModule } from './content-viewer-routing.module';
import { MatSelectModule } from '@angular/material/select';
import { StoreModule } from '@ngrx/store';
import { EffectsModule } from '@ngrx/effects';
import { ViewerOptionsEffects } from '../state/viewer-options/viewer-options.effects';
import { contentViewersFeatureKey, reducers } from '../state';
import { FormsModule, ReactiveFormsModule } from '@angular/forms';
import { HexViewer } from '../ui/hex-viewer/hex-viewer.component';
import { ImageViewer } from '../ui/image-viewer/image-viewer.component';
import { ContentEffects } from '../state/content/content.effects';
import { ExternalViewerEffects } from '../state/external-viewer/external-viewer.effects';
import { NifiTooltipDirective } from '@nifi/shared';

@NgModule({
    declarations: [ContentViewerComponent],
    exports: [ContentViewerComponent],
    imports: [
        CommonModule,
        ContentViewerRoutingModule,
        StoreModule.forFeature(contentViewersFeatureKey, reducers),
        EffectsModule.forFeature(ViewerOptionsEffects, ContentEffects, ExternalViewerEffects),
        MatSelectModule,
        FormsModule,
        ReactiveFormsModule,
        HexViewer,
        ImageViewer,
        NgOptimizedImage,
        NifiTooltipDirective
    ]
})
export class ContentViewerModule {}
