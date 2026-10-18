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

import { Injectable, inject, signal } from '@angular/core';
import { Storage } from '@nifi/shared';

const WORKSPACE_KEY = 'workspace-presentation';

@Injectable({ providedIn: 'root' })
export class WorkspacePreferencesService {
    private storage = inject(Storage);
    private modern = signal(this.readPreference());
    readonly enabled = this.modern.asReadonly();

    toggle(): void {
        const enabled = !this.modern();
        this.modern.set(enabled);
        try {
            if (enabled) this.storage.setItem(WORKSPACE_KEY, 'modern-v1', 0);
            else this.storage.removeItem(WORKSPACE_KEY);
        } catch {
            // Presentation can still change for this session when browser storage is unavailable.
        }
    }

    private readPreference(): boolean {
        try {
            return this.storage.getItem(WORKSPACE_KEY) === 'modern-v1';
        } catch {
            return false;
        }
    }
}
