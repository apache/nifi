import React, { useRef, useEffect } from 'react';
import { useSelector } from 'react-redux';
import { RootState } from '../state/store';
import { Activity, Plus, Settings, Play, Stop, Refresh } from 'lucide-react';

const FlowCanvas: React.FC = () => {
  const canvasRef = useRef<HTMLDivElement>(null);
  const systemDiagnostics = useSelector((state: RootState) => state.systemDiagnostics);
  const about = useSelector((state: RootState) => state.about);

  useEffect(() => {
    // Initialize canvas or flow diagram library here
    // This is where you would integrate with a flow diagram library like React Flow, or D3.js
  }, []);

  return (
    <div className="h-full flex flex-col bg-gray-50">
      {/* Flow Canvas Toolbar */}
      <div className="bg-white border-b border-gray-200 px-4 py-2 shadow-sm">
        <div className="flex items-center justify-between">
          <div className="flex items-center space-x-2">
            <button
              type="button"
              className="btn-outline px-3 py-2 rounded-md flex items-center space-x-2"
              title="Add Processor"
            >
              <Plus className="h-4 w-4" />
              <span>Processor</span>
            </button>
            <button
              type="button"
              className="btn-outline px-3 py-2 rounded-md flex items-center space-x-2"
              title="Add Input Port"
            >
              <Activity className="h-4 w-4" />
              <span>Input Port</span>
            </button>
            <button
              type="button"
              className="btn-outline px-3 py-2 rounded-md flex items-center space-x-2"
              title="Add Output Port"
            >
              <Activity className="h-4 w-4" />
              <span>Output Port</span>
            </button>
          </div>
          
          <div className="flex items-center space-x-2">
            <button
              type="button"
              className="btn-primary px-3 py-2 rounded-md flex items-center space-x-2"
              title="Start Flow"
            >
              <Play className="h-4 w-4" />
              <span>Start</span>
            </button>
            <button
              type="button"
              className="btn-secondary px-3 py-2 rounded-md flex items-center space-x-2"
              title="Stop Flow"
            >
              <Stop className="h-4 w-4" />
              <span>Stop</span>
            </button>
            <button
              type="button"
              className="btn-outline px-3 py-2 rounded-md"
              title="Refresh"
            >
              <Refresh className="h-4 w-4" />
            </button>
            <button
              type="button"
              className="btn-outline px-3 py-2 rounded-md"
              title="Settings"
            >
              <Settings className="h-4 w-4" />
            </button>
          </div>
        </div>
      </div>

      {/* Main Canvas Area */}
      <div className="flex-1 relative overflow-hidden">
        <div
          ref={canvasRef}
          className="absolute inset-0 bg-white"
          style={{
            backgroundImage: `
              linear-gradient(rgba(0,0,0,.1) 1px, transparent 1px),
              linear-gradient(90deg, rgba(0,0,0,.1) 1px, transparent 1px)
            `,
            backgroundSize: '20px 20px'
          }}
        >
          {/* Canvas content - this would be replaced with actual flow diagram components */}
          <div className="flex items-center justify-center h-full">
            <div className="text-center">
              <Activity className="h-16 w-16 text-gray-400 mx-auto mb-4" />
              <h3 className="text-lg font-medium text-gray-900 mb-2">Flow Canvas</h3>
              <p className="text-gray-500 mb-6 max-w-sm">
                Drag and drop processors, ports, and other components to build your data flow.
              </p>
              <div className="space-y-2">
                <button
                  type="button"
                  className="btn-primary px-4 py-2 rounded-md flex items-center space-x-2 mx-auto"
                >
                  <Plus className="h-4 w-4" />
                  <span>Add Your First Processor</span>
                </button>
                <p className="text-xs text-gray-400">
                  Or use the toolbar above to add components
                </p>
              </div>
            </div>
          </div>
        </div>
      </div>

      {/* Status Bar */}
      <div className="bg-white border-t border-gray-200 px-4 py-2 text-sm text-gray-600">
        <div className="flex items-center justify-between">
          <div className="flex items-center space-x-4">
            <span>NiFi {about.about?.niFiVersion || 'Unknown'}</span>
            {systemDiagnostics.systemDiagnostics && (
              <>
                <span>•</span>
                <span>
                  CPU: {systemDiagnostics.systemDiagnostics.processorLoadAverage?.toFixed(1) || 'N/A'}%
                </span>
                <span>•</span>
                <span>
                  Memory: {Math.round((systemDiagnostics.systemDiagnostics.usedHeap / systemDiagnostics.systemDiagnostics.maxHeap) * 100) || 'N/A'}%
                </span>
              </>
            )}
          </div>
          <div className="flex items-center space-x-2">
            <span className="inline-flex items-center px-2 py-1 rounded-full text-xs bg-green-100 text-green-800">
              <span className="w-1.5 h-1.5 bg-green-400 rounded-full mr-1.5"></span>
              Connected
            </span>
          </div>
        </div>
      </div>
    </div>
  );
};

export default FlowCanvas;