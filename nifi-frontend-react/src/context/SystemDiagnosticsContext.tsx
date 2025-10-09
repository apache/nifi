import { ReactNode, createContext, useContext } from 'react';

import {
  useSystemDiagnostics,
  type UseSystemDiagnosticsOptions,
  type UseSystemDiagnosticsResult
} from '../hooks/useSystemDiagnostics';

const SystemDiagnosticsContext = createContext<UseSystemDiagnosticsResult | undefined>(undefined);

interface SystemDiagnosticsProviderProps {
  children: ReactNode;
  options?: UseSystemDiagnosticsOptions;
}

export const SystemDiagnosticsProvider = ({
  children,
  options
}: SystemDiagnosticsProviderProps) => {
  const diagnostics = useSystemDiagnostics(options);

  return (
    <SystemDiagnosticsContext.Provider value={diagnostics}>
      {children}
    </SystemDiagnosticsContext.Provider>
  );
};

export function useSystemDiagnosticsContext(): UseSystemDiagnosticsResult {
  const context = useContext(SystemDiagnosticsContext);

  if (!context) {
    throw new Error('useSystemDiagnosticsContext must be used within a SystemDiagnosticsProvider');
  }

  return context;
}
