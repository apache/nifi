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
import { ExplorerComponent } from './explorer.component';
import { MatTableModule } from '@angular/material/table';
import { MatSortModule } from '@angular/material/sort';
import { MatMenuModule } from '@angular/material/menu';
import { DropletTableFilterComponent } from './ui/droplet-table-filter/droplet-table-filter.component';
import { MatButtonModule } from '@angular/material/button';
import { DeleteDropletDialogComponent } from './ui/delete-droplet-dialog/delete-droplet-dialog.component';

@NgModule({
    declarations: [ExplorerComponent],
    exports: [ExplorerComponent],
    imports: [
        CommonModule,
        MatTableModule,
        MatSortModule,
        MatMenuModule,
        DropletTableFilterComponent,
        MatButtonModule,
        NgOptimizedImage,
        MatButtonModule,
        DeleteDropletDialogComponent
    ]
})
export class ExplorerModule {}
