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

import { Dimension } from './canvas.types';

export class CanvasConstants {
    static readonly PROCESSOR: Readonly<Dimension> = {
        width: 350,
        height: 130
    };

    static readonly FUNNEL: Readonly<Dimension> = {
        width: 48,
        height: 48
    };

    static readonly PORT: Readonly<Dimension> = {
        width: 240,
        height: 48
    };

    static readonly REMOTE_PORT: Readonly<Dimension> = {
        width: 240,
        height: 80
    };

    static readonly PROCESS_GROUP: Readonly<Dimension> = {
        width: 384,
        height: 176
    };

    static readonly REMOTE_PROCESS_GROUP: Readonly<Dimension> = {
        width: 384,
        height: 176
    };

    static readonly LABEL_MIN: Readonly<Dimension> = {
        width: 64,
        height: 24
    };

    static readonly CONNECTION_LABEL: Readonly<Dimension> = {
        width: 240,
        height: 0
    };

    static readonly SNAP_ALIGNMENT_PIXELS = 8;

    static readonly LABEL_DEFAULT_COLOR = '#fff7d7';

    static readonly PORT_REMOTE_BANNER_HEIGHT = 25;
    static readonly PORT_OFFSET_VALUE = 25;

    static readonly PROCESS_GROUP_BANNER_HEIGHT = 32;
    static readonly PROCESS_GROUP_CONTENTS_BANNER_HEIGHT = 24;
    static readonly PROCESS_GROUP_STATS_ROW_HEIGHT = 19;
    static readonly PROCESS_GROUP_CONTENTS_SPACER = 10;
    static readonly PROCESS_GROUP_CONTENTS_VALUE_SPACER = 5;

    static readonly REMOTE_PROCESS_GROUP_BANNER_HEIGHT = 32;
    static readonly REMOTE_PROCESS_GROUP_DETAILS_BANNER_HEIGHT = 24;
    static readonly REMOTE_PROCESS_GROUP_STATS_ROW_HEIGHT = 19;

    static readonly CONNECTION_ROW_HEIGHT = 19;
    static readonly CONNECTION_HEIGHT_FOR_BACKPRESSURE = 3;
    static readonly CONNECTION_BACKPRESSURE_BAR_WIDTH = 103;
    static readonly CONNECTION_BACKPRESSURE_COUNT_OFFSET = 6;
    static readonly CONNECTION_BACKPRESSURE_DATASIZE_OFFSET = 131;
}
