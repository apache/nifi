import { createSlice, PayloadAction } from '@reduxjs/toolkit';

export interface RepositoryStorageUsage {
  freeSpace: string;
  freeSpaceBytes: number;
  totalSpace: string;
  totalSpaceBytes: number;
  usedSpace: string;
  usedSpaceBytes: number;
  utilization: string;
  identifier?: string;
}

export interface GarbageCollection {
  collectionCount: number;
  collectionMillis: number;
  collectionTime: string;
  name: string;
}

export interface VersionInfo {
  buildBranch: string;
  buildRevision: string;
  buildTag: string;
  buildTimestamp: string;
  javaVendor: string;
  javaVersion: string;
  niFiVersion: string;
  osArchitecture: string;
  osName: string;
  osVersion: string;
}

export interface SystemDiagnosticSnapshot {
  availableProcessors: number;
  contentRepositoryStorageUsage: RepositoryStorageUsage[];
  daemonThreads: number;
  flowFileRepositoryStorageUsage: RepositoryStorageUsage;
  freeHeap: string;
  freeHeapBytes: number;
  freeNonHeap: string;
  freeNonHeapBytes: number;
  garbageCollection: GarbageCollection[];
  heapUtilization: string;
  maxHeap: string;
  maxHeapBytes: number;
  maxNonHeap: string;
  maxNonHeapBytes: number;
  processorLoadAverage: number;
  provenanceRepositoryStorageUsage: RepositoryStorageUsage[];
  statsLastRefreshed: string;
  totalHeap: string;
  totalHeapBytes: number;
  totalNonHeap: string;
  totalNonHeapBytes: number;
  totalThreads: number;
  uptime: string;
  usedHeap: string;
  usedHeapBytes: number;
  usedNonHeap: string;
  usedNonHeapBytes: number;
  versionInfo: VersionInfo;
}

export interface NodeSnapshot {
  address: string;
  apiPort: number;
  nodeId: string;
  snapshot: SystemDiagnosticSnapshot;
}

export interface SystemDiagnostics {
  aggregateSnapshot: SystemDiagnosticSnapshot;
  nodeSnapshots?: NodeSnapshot[];
}

export interface SystemDiagnosticsState {
  systemDiagnostics?: SystemDiagnostics;
  loadedTimestamp?: string;
  status: 'idle' | 'loading' | 'success' | 'error';
  error?: string;
}

const initialState: SystemDiagnosticsState = {
  status: 'idle'
};

const systemDiagnosticsSlice = createSlice({
  name: 'systemDiagnostics',
  initialState,
  reducers: {
    setSystemDiagnosticsStatus(state, action: PayloadAction<SystemDiagnosticsState['status']>) {
      state.status = action.payload;
      if (action.payload !== 'error') {
        state.error = undefined;
      }
    },
    setSystemDiagnostics(state, action: PayloadAction<SystemDiagnostics | undefined>) {
      state.systemDiagnostics = action.payload;
      state.loadedTimestamp = action.payload?.aggregateSnapshot.statsLastRefreshed;
    },
    setSystemDiagnosticsError(state, action: PayloadAction<string | undefined>) {
      state.error = action.payload ?? 'Unknown error';
      state.status = 'error';
    },
    resetSystemDiagnostics() {
      return initialState;
    }
  }
});

export const {
  setSystemDiagnosticsStatus,
  setSystemDiagnostics,
  setSystemDiagnosticsError,
  resetSystemDiagnostics
} = systemDiagnosticsSlice.actions;

export default systemDiagnosticsSlice.reducer;
