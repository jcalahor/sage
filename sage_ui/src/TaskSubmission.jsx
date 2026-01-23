import { useState } from 'react';
import './TaskSubmission.css';

function TaskSubmission() {
  const [jsonInput, setJsonInput] = useState('{\n  "requestor_id": 10,\n  "task_name": "PrimeTask",\n  "task_envelope": "{\\"limit\\":2500}"\n}');
  const [response, setResponse] = useState(null);
  const [error, setError] = useState(null);
  const [loading, setLoading] = useState(false);

  const handleSubmit = async (e) => {
    e.preventDefault();
    setLoading(true);
    setError(null);
    setResponse(null);

    try {
      // Parse JSON to validate it
      const parsedJson = JSON.parse(jsonInput);
      
      // Make POST request to the API
      const res = await fetch('http://localhost:4000/tasks/v1/start', {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
        },
        body: JSON.stringify(parsedJson),
      });

      const data = await res.json();
      
      if (res.ok) {
        setResponse(data);
      } else {
        setError(`Error ${res.status}: ${JSON.stringify(data, null, 2)}`);
      }
    } catch (err) {
      if (err instanceof SyntaxError) {
        setError('Invalid JSON format. Please check your input.');
      } else {
        setError(`Request failed: ${err.message}`);
      }
    } finally {
      setLoading(false);
    }
  };

  const handleClear = () => {
    setJsonInput('');
    setResponse(null);
    setError(null);
  };

  const handleReset = () => {
    setJsonInput('{\n  "requestor_id": 10,\n  "task_name": "PrimeTask",\n  "task_envelope": "{\\"limit\\":2500}"\n}');
    setResponse(null);
    setError(null);
  };

  return (
    <div className="task-submission">
      <h2>Submit Task</h2>
      <p className="endpoint-info">
        Endpoint: <code>POST http://localhost:4000/tasks/v1/start</code>
      </p>
      
      <form onSubmit={handleSubmit}>
        <div className="form-group">
          <label htmlFor="json-input">JSON Payload:</label>
          <textarea
            id="json-input"
            value={jsonInput}
            onChange={(e) => setJsonInput(e.target.value)}
            rows={12}
            placeholder='{"requestor_id": 10, "task_name": "PrimeTask", "task_envelope": "{\"limit\":2500}"}'
            disabled={loading}
          />
        </div>
        
        <div className="button-group">
          <button type="submit" disabled={loading}>
            {loading ? 'Submitting...' : 'Submit Task'}
          </button>
          <button type="button" onClick={handleReset} disabled={loading}>
            Reset to Example
          </button>
          <button type="button" onClick={handleClear} disabled={loading}>
            Clear All
          </button>
        </div>
      </form>

      {error && (
        <div className="error-box">
          <h3>Error:</h3>
          <pre>{error}</pre>
        </div>
      )}

      {response && (
        <div className="response-box">
          <h3>Response:</h3>
          <pre>{JSON.stringify(response, null, 2)}</pre>
        </div>
      )}
    </div>
  );
}

export default TaskSubmission;
