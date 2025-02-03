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

import { Component, EventEmitter, Input, Output } from '@angular/core';
import { CommonModule } from '@angular/common';
import { MatButton } from '@angular/material/button';
import { MatDialogActions, MatDialogClose, MatDialogContent, MatDialogTitle } from '@angular/material/dialog';
import { MatProgressBar } from '@angular/material/progress-bar';
import { PropertyVerificationRequest } from '../../../../../state/property-verification';
import { Observable, of } from 'rxjs';

@Component({
    selector: 'property-verification-progress',
    imports: [
        CommonModule,
        MatButton,
        MatDialogActions,
        MatDialogClose,
        MatDialogContent,
        MatDialogTitle,
        MatProgressBar
    ],
    templateUrl: './property-verification-progress.component.html',
    styleUrl: './property-verification-progress.component.scss'
})
export class PropertyVerificationProgress {
    @Input() verificationRequest$: Observable<PropertyVerificationRequest | null> = of(null);
    @Output() stopVerification = new EventEmitter<PropertyVerificationRequest>();

    stop(request: PropertyVerificationRequest) {
        this.stopVerification.next(request);
    }
}
