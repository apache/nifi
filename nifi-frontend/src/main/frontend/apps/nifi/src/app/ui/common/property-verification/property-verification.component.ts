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
import { MatIconButton } from '@angular/material/button';
import { ConfigVerificationResult, Outcome } from '../../../state/property-verification';

@Component({
    selector: 'property-verification',
    imports: [CommonModule, MatIconButton],
    templateUrl: './property-verification.component.html',
    styleUrl: './property-verification.component.scss'
})
export class PropertyVerification {
    private _results: ConfigVerificationResult[] | null = null;

    @Input() set results(results: ConfigVerificationResult[] | null) {
        this._results = results;
    }
    get results(): ConfigVerificationResult[] {
        return this._results || [];
    }

    @Input() isVerifying = false;
    @Input() disabled = false;

    @Output() verify: EventEmitter<void> = new EventEmitter<void>();

    verifyClicked(): void {
        this.verify.next();
    }

    protected readonly Outcome = Outcome;
}
