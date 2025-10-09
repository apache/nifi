import React, { useEffect } from 'react';
import { useDispatch } from 'react-redux';
import { Outlet } from 'react-router-dom';
import { loadExtensionTypesForCanvas } from '../state/extensionTypesSlice';
import BannerText from '../components/BannerText';
import { useAbout } from '../hooks/useAbout';
import { SystemDiagnosticsProvider } from '../context/SystemDiagnosticsContext';

const FlowDesigner = () => {
  const dispatch = useDispatch();
  useAbout();

  useEffect(() => {
    dispatch(loadExtensionTypesForCanvas());
  }, [dispatch]);

  return (
    <BannerText>
      <SystemDiagnosticsProvider options={{ autoLoad: false }}>
        <Outlet />
      </SystemDiagnosticsProvider>
    </BannerText>
  );
};

export default FlowDesigner;
