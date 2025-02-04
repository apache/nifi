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

import { Component } from '@angular/core';
import { MatButtonModule } from '@angular/material/button';
import { MatDialogModule } from '@angular/material/dialog';
import { MatTableModule } from '@angular/material/table';
import { MatSortModule } from '@angular/material/sort';
import { AsyncPipe, NgOptimizedImage } from '@angular/common';
import { CloseOnEscapeDialog } from '@nifi/shared';
import { Store } from '@ngrx/store';
import { ReactiveFormsModule } from '@angular/forms';
import { MatFormFieldModule } from '@angular/material/form-field';
import { MatInputModule } from '@angular/material/input';
import { AboutState } from '../../../state/about';
import { selectAbout } from '../../../state/about/about.selectors';

@Component({
    selector: 'about',
    templateUrl: './about-dialog.component.html',
    imports: [
        MatButtonModule,
        MatDialogModule,
        MatTableModule,
        MatSortModule,
        AsyncPipe,
        ReactiveFormsModule,
        MatFormFieldModule,
        MatInputModule,
        NgOptimizedImage
    ],
    styleUrls: ['./about-dialog.component.scss']
})
export class AboutDialog extends CloseOnEscapeDialog {
    about$ = this.store.select(selectAbout);

    constructor(private store: Store<AboutState>) {
        super();
    }
}
