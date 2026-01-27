import { useState, useEffect } from 'react';
import './JobsList.css';

function JobsList() {
  const [schedules, setSchedules] = useState([]);
  const [error, setError] = useState(null);
  const [loading, setLoading] = useState(false);
  const [requestorId, setRequestorId] = useState('');
  const [autoRefresh, setAutoRefresh] = useState(false);

  const fetchSchedules = async () => {
    setLoading(true);
    setError(null);

    try {
      const payload = {};
      if (requestorId.trim() !== '') {
        payload.requestor_id = parseInt(requestorId, 10);
        if (isNaN(payload.requestor_id)) {
          setError('Invalid requestor ID. Please enter a valid number.');
          setLoading(false);
          return;
        }
      }

      const res = await fetch('http://localhost:4000/jobs/v1/list', {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
        },
        body: JSON.stringify(payload),
      });

      const data = await res.json();
      
      if (res.ok) {
        setSchedules(data.jobs || []);
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
      fetchSchedules();
      const interval = setInterval(fetchSchedules, 5000);
      return () => clearInterval(interval);
    }
  }, [autoRefresh, requestorId]);

  const formatDateTime = (dateString) => {
    if (!dateString) return 'N/A';
    const date = new Date(dateString);
    return date.toLocaleString();
  };

  return (
    <div className="jobs-list">
      <h2>Jobs</h2>
      
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
          <button onClick={fetchSchedules} disabled={loading}>
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

      {!error && schedules.length === 0 && !loading && (
        <div className="info-box">
          No jobs found. Click "Refresh" to load schedules.
        </div>
      )}

      {schedules.length > 0 && (
        <div className="grid-container">
          <div className="count-info">
            Total: <strong>{schedules.length}</strong> job(s)
          </div>
          
          <div className="table-wrapper">
            <table className="data-grid">
              <thead>
                <tr>
                  <th>Status</th>
                  <th>Schedule Name</th>
                  <th>Task Name</th>
                  <th>Cron Expression</th>
                  <th>Timezone</th>
                  <th>Next Run</th>
                  <th>Last Run</th>
                  <th>Priority</th>
                  <th>Requestor ID</th>
                  <th>ID</th>
                </tr>
              </thead>
              <tbody>
                {schedules.map((schedule) => (
                  <tr key={schedule.id}>
                    <td>
                      <span className={`status-badge ${schedule.enabled ? 'enabled' : 'disabled'}`}>
                        {schedule.enabled ? 'Enabled' : 'Disabled'}
                      </span>
                    </td>
                    <td className="text-left">{schedule.schedule_name}</td>
                    <td className="text-left">{schedule.task_name}</td>
                    <td className="monospace">{schedule.cron_expression}</td>
                    <td>{schedule.timezone}</td>
                    <td>{formatDateTime(schedule.next_run_at)}</td>
                    <td>{formatDateTime(schedule.last_run_at)}</td>
                    <td>{schedule.priority}</td>
                    <td>{schedule.requestor_id}</td>
                    <td className="monospace small">{schedule.id}</td>
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

export default JobsList;
