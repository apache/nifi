import { createSelector } from '@reduxjs/toolkit';

import type { RootState } from './store';
import type { SystemDiagnosticsState } from './systemDiagnosticsSlice';

export const selectSystemDiagnosticsState = (state: RootState): SystemDiagnosticsState => state.systemDiagnostics;

export const selectSystemDiagnostics = createSelector(
  selectSystemDiagnosticsState,
  (state) => state.systemDiagnostics
);

export const selectSystemDiagnosticsStatus = createSelector(
  selectSystemDiagnosticsState,
  (state) => state.status
);

export const selectSystemDiagnosticsLoadedTimestamp = createSelector(
  selectSystemDiagnosticsState,
  (state) => state.loadedTimestamp
);

export const selectSystemNodeSnapshots = createSelector(
  selectSystemDiagnosticsState,
  (state) => state.systemDiagnostics?.nodeSnapshots
);
