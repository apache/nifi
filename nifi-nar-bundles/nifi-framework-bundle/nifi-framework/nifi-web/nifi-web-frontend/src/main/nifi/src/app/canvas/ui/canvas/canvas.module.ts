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
import { CanvasComponent } from './canvas.component';
import { ContextMenu } from './context-menu/context-menu.component';
import { CdkContextMenuTrigger, CdkMenu, CdkMenuItem, CdkMenuTrigger } from '@angular/cdk/menu';
import { EditCanvasItem } from '../edit-canvas-item/edit-canvas-item.component';
import { SelectCanvasItem } from '../select-canvas-item/select-canvas-item.component';
import { BulkSelectCanvasItems } from '../bulk-select-canvas-items/bulk-select-canvas-items.component';

@NgModule({
    declarations: [CanvasComponent, ContextMenu, EditCanvasItem, SelectCanvasItem, BulkSelectCanvasItems],
    exports: [CanvasComponent],
    imports: [CommonModule, CdkMenu, CdkMenuItem, CdkMenuTrigger, CdkContextMenuTrigger]
})
export class CanvasModule {}
