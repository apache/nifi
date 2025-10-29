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

import { AsyncPipe, NgOptimizedImage } from '@angular/common';
import { Component, inject } from '@angular/core';
import { ReactiveFormsModule } from '@angular/forms';
import { MatButtonModule } from '@angular/material/button';
import { MatDialogModule } from '@angular/material/dialog';
import { MatFormFieldModule } from '@angular/material/form-field';
import { MatInputModule } from '@angular/material/input';
import { CloseOnEscapeDialog } from '@nifi/shared';
import { Store } from '@ngrx/store';
import { AboutState } from '../../../state/about';
import { selectAbout } from '../../../state/about/about.selectors';

@Component({
    selector: 'registry-about-dialog',
    templateUrl: './about-dialog.component.html',
    styleUrl: './about-dialog.component.scss',
    standalone: true,
    imports: [
        MatButtonModule,
        MatDialogModule,
        AsyncPipe,
        ReactiveFormsModule,
        MatFormFieldModule,
        MatInputModule,
        NgOptimizedImage
    ]
})
export class AboutDialogComponent extends CloseOnEscapeDialog {
    private store = inject<Store<AboutState>>(Store);

    constructor() {
        super();
    }

    about$ = this.store.select(selectAbout);
}
