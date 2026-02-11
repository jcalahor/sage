import { useState, useEffect } from 'react';
import './TasksList.css';

function TasksList() {
  const [tasks, setTasks] = useState([]);
  const [error, setError] = useState(null);
  const [loading, setLoading] = useState(false);
  const [requestorId, setRequestorId] = useState('');
  const [autoRefresh, setAutoRefresh] = useState(false);

  const fetchTasks = async () => {
    setLoading(true);
    setError(null);

    try {
      let url = 'http://localhost:4000/tasks/v1/list';
      
      if (requestorId.trim() !== '') {
        const parsedId = parseInt(requestorId, 10);
        if (isNaN(parsedId)) {
          setError('Invalid requestor ID. Please enter a valid number.');
          setLoading(false);
          return;
        }
        url += `?requestor_id=${parsedId}`;
      }

      const res = await fetch(url, {
        method: 'GET',
        headers: {
          'Content-Type': 'application/json',
        },
      });

      const data = await res.json();
      
      if (res.ok) {
        setTasks(data.tasks || []);
      } else {
        setError(`Error ${res.status}: ${JSON.stringify(data, null, 2)}`);
      }
    } catch (err) {
      setError(`Request failed: ${err.message}`);
    } finally {
      setLoading(false);
    }
  };

  useEffect(() => {
    if (autoRefresh) {
      fetchTasks();
      const interval = setInterval(fetchTasks, 5000);
      return () => clearInterval(interval);
    }
  }, [autoRefresh, requestorId]);

  const formatDateTime = (dateString) => {
    if (!dateString) return 'N/A';
    const date = new Date(dateString);
    return date.toLocaleString();
  };

  const getStatusClass = (status) => {
    switch (status) {
      case 'completed':
        return 'status-completed';
      case 'pending':
        return 'status-pending';
      case 'error':
        return 'status-error';
      default:
        return '';
    }
  };

  const formatJSON = (jsonString) => {
    if (!jsonString) return 'N/A';
    try {
      const parsed = typeof jsonString === 'string' ? JSON.parse(jsonString) : jsonString;
      return JSON.stringify(parsed, null, 2);
    } catch {
      return jsonString;
    }
  };

  return (
    <div className="tasks-list">
      <h2>Tasks</h2>
      
      <div className="controls">
        <div className="filter-group">
          <label htmlFor="requestor-id">Filter by Requestor ID:</label>
          <input
            id="requestor-id"
            type="text"
            value={requestorId}
            onChange={(e) => setRequestorId(e.target.value)}
            placeholder="Leave empty for all"
            disabled={loading}
          />
        </div>
        
        <div className="button-group">
          <button onClick={fetchTasks} disabled={loading}>
            {loading ? 'Loading...' : 'Refresh'}
          </button>
          <label className="auto-refresh-toggle">
            <input
              type="checkbox"
              checked={autoRefresh}
              onChange={(e) => setAutoRefresh(e.target.checked)}
            />
            Auto-refresh (5s)
          </label>
        </div>
      </div>

      {error && (
        <div className="error-box">
          <strong>Error:</strong> {error}
        </div>
      )}

      {!error && tasks.length === 0 && !loading && (
        <div className="info-box">
          No tasks found. Click "Refresh" to load tasks.
        </div>
      )}

      {tasks.length > 0 && (
        <div className="grid-container">
          <div className="count-info">
            Total: <strong>{tasks.length}</strong> task(s)
          </div>
          
          <div className="table-wrapper">
            <table className="data-grid">
              <thead>
                <tr>
                  <th>Status</th>
                  <th>Task Name</th>
                  <th>Priority</th>
                  <th>Retry Count</th>
                  <th>Created At</th>
                  <th>Started At</th>
                  <th>Completed At</th>
                  <th>Input JSON</th>
                  <th>Output JSON</th>
                  <th>Worker ID</th>
                  <th>Requestor ID</th>
                  <th>Task ID</th>
                </tr>
              </thead>
              <tbody>
                {tasks.map((task) => (
                  <tr key={task.id}>
                    <td>
                      <span className={`status-badge ${getStatusClass(task.status)}`}>
                        {task.status}
                      </span>
                    </td>
                    <td className="text-left">{task.task_name}</td>
                    <td>{task.priority}</td>
                    <td>{task.retry_count} / {task.max_retries}</td>
                    <td>{formatDateTime(task.created_at)}</td>
                    <td>{formatDateTime(task.started_at)}</td>
                    <td>{formatDateTime(task.completed_at)}</td>
                    <td className="json-cell">
                      <pre className="json-content">{formatJSON(task.task_context)}</pre>
                    </td>
                    <td className="json-cell">
                      <pre className="json-content">{formatJSON(task.result)}</pre>
                    </td>
                    <td className="monospace small">{task.worker_id || 'N/A'}</td>
                    <td>{task.requestor_id}</td>
                    <td className="monospace small">{task.id}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        </div>
      )}
    </div>
  );
}

export default TasksList;
