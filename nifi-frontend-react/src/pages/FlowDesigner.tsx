import React, { useEffect } from 'react';
import { useDispatch } from 'react-redux';
import { Routes, Route } from 'react-router-dom';
import { loadExtensionTypesForCanvas } from '../state/extensionTypesSlice';
import { useAbout } from '../hooks/useAbout';
import { SystemDiagnosticsProvider } from '../context/SystemDiagnosticsContext';
import FlowCanvas from '../components/FlowCanvas';

const FlowDesigner: React.FC = () => {
  const dispatch = useDispatch();
  useAbout();

  useEffect(() => {
    dispatch(loadExtensionTypesForCanvas());
  }, [dispatch]);

  return (
    <SystemDiagnosticsProvider options={{ autoLoad: false }}>
      <div className="h-full flex flex-col">
        <div className="flex-1 bg-gray-100">
          <Routes>
            <Route path="/" element={<FlowCanvas />} />
            <Route path="*" element={<FlowCanvas />} />
          </Routes>
        </div>
      </div>
    </SystemDiagnosticsProvider>
  );
};

export default FlowDesigner;
