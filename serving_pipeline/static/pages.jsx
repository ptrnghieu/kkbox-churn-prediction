// pages.jsx — Page components for KKBox Dashboard (with real API calls + polling)
// Exports to window: SingleUserPage, BatchPage, StatisticsPage, ModelInfoPage, APIHealthPage

const { useState, useEffect, useRef } = React;

// ─── API Helpers ──────────────────────────────────────────────────────────────
async function apiGet(base, path) {
  const r = await fetch(`${base}${path}`, { signal: AbortSignal.timeout(5000) });
  if (!r.ok) throw new Error(`HTTP ${r.status}`);
  return r.json();
}
async function apiPost(base, path, body, timeoutMs = 30000) {
  const r = await fetch(`${base}${path}`, {
    method: 'POST', headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify(body), signal: AbortSignal.timeout(timeoutMs),
  });
  if (!r.ok) throw new Error(`HTTP ${r.status}`);
  return r.json();
}
function parseCsv(text) {
  const lines = text.trim().split(/\r?\n/);
  const hdrs = lines[0].split(',').map(h => h.trim().replace(/^"|"$/g, '').toLowerCase());
  const idx = hdrs.indexOf('msno');
  if (idx < 0) return null;
  return lines.slice(1).map(l => l.split(',')[idx]?.trim().replace(/^"|"$/g, '')).filter(Boolean);
}

// ─── Mock Data ────────────────────────────────────────────────────────────────
function randomB64() {
  const c = 'ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/';
  return Array.from({ length: 43 }, () => c[Math.floor(Math.random() * c.length)]).join('') + '=';
}
function hashFloat(s) {
  let h = 2166136261;
  for (let i = 0; i < s.length; i++) h = Math.imul(h ^ s.charCodeAt(i), 16777619);
  return (h >>> 0) / 4294967295;
}
function fmtMsno(s) { return s.length > 22 ? s.slice(0, 10) + '…' + s.slice(-6) : s; }
function mockPredict(msno) {
  const b = hashFloat(msno);
  const prob = b < 0.10 ? 0.82 + b * 0.18 : b * 0.85;
  return { msno, churn_probability: prob, is_churn: prob >= THRESHOLD ? 1 : 0, member_found: b > 0.05 };
}
function genBatch(n = 600) {
  return Array.from({ length: n }, () => {
    const r = Math.random();
    const prob = r < 0.10 ? 0.80 + Math.random() * 0.18 : r < 0.22 ? 0.45 + Math.random() * 0.34 : Math.random() * 0.38;
    return { msno: randomB64(), churn_probability: prob, is_churn: prob >= THRESHOLD ? 1 : 0 };
  });
}
const SAMPLE_MSNOS = Array.from({ length: 6 }, randomB64);
const MOCK_STATS = { total_predictions: 4832, churn_count: 412, retain_count: 4420, churn_rate: 0.0852 };
const MOCK_CHURNED = Array.from({ length: 24 }, (_, i) => {
  const msno = randomB64();
  const d = new Date(2026, 4, 28 - (i % 10), 8 + (i % 14), (i * 7) % 60);
  return { msno, churn_probability: 0.80 + Math.random() * 0.18, predicted_at: d.toISOString().replace('T', ' ').slice(0, 19) };
});

// ─── Shared Components ────────────────────────────────────────────────────────
function StatBox({ value, label, variant = '' }) {
  return (
    <div className="stat-box">
      <div className={`stat-value ${variant}`}>{value}</div>
      <div className="stat-label">{label}</div>
    </div>
  );
}
function Badge({ isChurn }) {
  return isChurn
    ? <span className="badge churn">⚠ High churn risk</span>
    : <span className="badge retain">✓ Low churn risk</span>;
}
function CardTitle({ children }) { return <div className="card-title">{children}</div>; }
function Spinner() {
  return (
    <div style={{ display: 'flex', alignItems: 'center', gap: 3, height: 30 }}>
      {[0.1, 0.2, 0.3, 0.1, 0.2].map((d, i) => (
        <div key={i} style={{ width: 3.5, borderRadius: 2, background: '#CF6F3C', opacity: 0.8,
          animation: `wf 0.9s ease-in-out ${d}s infinite`, height: [10, 20, 28, 16, 22][i] }} />
      ))}
    </div>
  );
}
function OfflineBanner() {
  return (
    <div style={{ background: '#fffbeb', border: '1px solid #fde68a', borderRadius: 8,
      padding: '0.55rem 0.85rem', fontSize: '0.77rem', color: '#92400e', marginBottom: '0.9rem' }}>
      ⚡ API offline — showing simulated data. Set the correct API URL in the Tweaks panel.
    </div>
  );
}

// ─── Contributing Factors ─────────────────────────────────────────────────────
const FEATURE_LABELS = {
  days_since_expiry: 'Days since expiry',  total_secs: 'Total listen time',
  total_log_days: 'Subscription length',   total_num_25: 'Tracks played >25%',
  total_num_unq: 'Unique songs played',    total_transactions: 'Total transactions',
  auto_renew_count: 'Auto-renewals',       cancel_count: 'Cancellations',
  avg_daily_secs: 'Avg daily listen time', total_amount_paid: 'Amount paid',
};
function mockShap(msno, prob) {
  const isHigh = prob >= THRESHOLD;
  return Object.entries(FEATURE_LABELS).map(([key, label], i) => {
    const h = hashFloat(msno + i);
    const mag = 0.04 + h * 0.24;
    const sign = isHigh ? (i < 2 ? 1 : h > 0.5 ? 1 : -1) : (i < 2 ? -1 : h > 0.4 ? -1 : 1);
    return { key, label, shap: sign * mag };
  }).sort((a, b) => Math.abs(b.shap) - Math.abs(a.shap));
}
function ContributingFactors({ msno, prob, shapValues = null }) {
  const factors = shapValues
    ? Object.entries(shapValues)
        .map(([key, shap]) => ({ key, label: FEATURE_LABELS[key] || key, shap }))
        .sort((a, b) => Math.abs(b.shap) - Math.abs(a.shap))
        .slice(0, 8)
    : mockShap(msno, prob).slice(0, 8);
  const maxAbs = Math.max(...factors.map(f => Math.abs(f.shap)), 0.01);
  return (
    <div style={{ marginTop: '1.25rem' }}>
      <CardTitle>Contributing Factors{shapValues ? '' : ' (mock SHAP)'}</CardTitle>
      <div style={{ fontSize: '0.74rem', color: '#9ca3af', marginBottom: '0.75rem', marginTop: '-0.5rem' }}>
        Top features influencing this prediction
      </div>
      <div style={{ display: 'flex', flexDirection: 'column', gap: 7 }}>
        {factors.map(f => {
          const pct = Math.abs(f.shap) / maxAbs;
          const isPos = f.shap > 0;
          return (
            <div key={f.key} style={{ display: 'grid', gridTemplateColumns: '140px 1fr 52px', alignItems: 'center', gap: 8 }}>
              <span style={{ fontSize: '0.78rem', color: '#374151', textAlign: 'right', fontFamily: 'monospace', overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap' }}>
                {f.label}
              </span>
              <div style={{ background: '#f3f4f6', borderRadius: 4, height: 14, overflow: 'hidden' }}>
                <div style={{ height: '100%', borderRadius: 4, width: `${pct * 100}%`,
                  background: isPos ? '#fca5a5' : '#86efac', transition: 'width 0.5s ease' }} />
              </div>
              <span style={{ fontSize: '0.73rem', fontWeight: 600, color: isPos ? '#dc2626' : '#16a34a',
                textAlign: 'right', fontVariantNumeric: 'tabular-nums' }}>
                {isPos ? '+' : '−'}{Math.abs(f.shap).toFixed(3)}
              </span>
            </div>
          );
        })}
      </div>
      <div style={{ display: 'flex', gap: 16, marginTop: '0.65rem', fontSize: '0.72rem', color: '#9ca3af' }}>
        <span style={{ display: 'flex', alignItems: 'center', gap: 5 }}>
          <span style={{ width: 10, height: 10, background: '#fca5a5', borderRadius: 2, display: 'inline-block' }} />Increases churn risk
        </span>
        <span style={{ display: 'flex', alignItems: 'center', gap: 5 }}>
          <span style={{ width: 10, height: 10, background: '#86efac', borderRadius: 2, display: 'inline-block' }} />Decreases churn risk
        </span>
      </div>
    </div>
  );
}

// ─── Single User Page ─────────────────────────────────────────────────────────
function SingleUserPage({ apiUrl = '', selectedMsno = '' }) {
  const [msno, setMsno] = useState(selectedMsno || '');
  const [loading, setLoading] = useState(false);
  const [result, setResult] = useState(null);
  const [offline, setOffline] = useState(false);

  useEffect(() => {
    if (selectedMsno) { setMsno(selectedMsno); setResult(null); }
  }, [selectedMsno]);

  const handleRandom = async () => {
    try {
      const data = await apiGet(apiUrl, '/sample?n=1');
      const id = data.msnos?.[0];
      if (id) { setMsno(id); setResult(null); return; }
    } catch {}
    setMsno(SAMPLE_MSNOS[Math.floor(Math.random() * SAMPLE_MSNOS.length)]);
    setResult(null);
  };

  const handlePredict = async (e) => {
    e.preventDefault();
    if (!msno.trim()) return;
    setLoading(true); setResult(null);
    let res, shapValues = null;
    try {
      res = await apiPost(apiUrl, '/predict/', { msno });
      setOffline(false);
    } catch {
      res = mockPredict(msno);
      setOffline(true);
    }
    // Try /explain for real SHAP values
    try {
      const exp = await apiPost(apiUrl, '/explain', { msno });
      shapValues = exp.shap_values || null;
    } catch {}
    setResult({ ...res, shapValues });
    setLoading(false);
  };

  const isChurn = result?.is_churn === 1;
  const scoreColor = result ? (isChurn ? '#dc2626' : '#16a34a') : '#9ca3af';

  return (
    <div className="two-col" style={{ alignItems: 'start' }}>
      <div>
        <CardTitle>Member Lookup</CardTitle>
        {offline && <OfflineBanner />}
        <button className="btn btn-secondary" style={{ marginBottom: '0.75rem', fontSize: '0.8rem' }} onClick={handleRandom}>
          🎲 Random sample
        </button>
        <form onSubmit={handlePredict}>
          <label style={{ display: 'block', fontSize: '0.78rem', fontWeight: 600, color: '#374151', marginBottom: '0.4rem' }}>
            Member ID (msno)
          </label>
          <input className="input" style={{ marginBottom: '0.75rem', fontFamily: 'monospace', fontSize: '0.8rem' }}
            value={msno} onChange={e => { setMsno(e.target.value); setResult(null); }}
            placeholder="e.g. Rbxl3f+P5SH7sYi6wI8aB…" />
          <button type="submit" className="btn btn-primary" style={{ width: '100%' }} disabled={loading || !msno.trim()}>
            {loading ? 'Predicting…' : 'Predict churn risk'}
          </button>
        </form>
        <p style={{ fontSize: '0.76rem', color: '#9ca3af', marginTop: '0.75rem', lineHeight: 1.5 }}>
          Enter a hashed member ID to look up features from the online store and get a real-time prediction.
        </p>
        {result && (
          <div style={{ marginTop: '1rem', background: '#fafaf9', borderRadius: 10, padding: '0.85rem 1rem', border: '1px solid #e5e7eb' }}>
            <div style={{ fontSize: '0.72rem', color: '#9ca3af', fontWeight: 700, textTransform: 'uppercase', letterSpacing: '0.08em', marginBottom: '0.5rem' }}>Details</div>
            {[['Member ID', fmtMsno(result.msno)], ['Score', `${(result.churn_probability * 100).toFixed(2)}%`], ['Threshold', '78.9%'], ['Found in store', result.member_found ? 'Yes' : 'No (avg features used)']].map(([k, v]) => (
              <div key={k} style={{ display: 'flex', justifyContent: 'space-between', padding: '0.3rem 0', borderBottom: '1px solid #f3f4f6', fontSize: '0.8rem' }}>
                <span style={{ color: '#6b7280' }}>{k}</span>
                <span style={{ color: '#111827', fontWeight: 600, fontFamily: k === 'Member ID' ? 'monospace' : 'inherit' }}>{v}</span>
              </div>
            ))}
          </div>
        )}
      </div>

      <div>
        <CardTitle>Prediction Result</CardTitle>
        {loading ? (
          <div style={{ display: 'flex', flexDirection: 'column', alignItems: 'center', justifyContent: 'center', height: 200, gap: 12 }}>
            <Spinner /><span style={{ fontSize: '0.8rem', color: '#9ca3af' }}>Running inference…</span>
          </div>
        ) : result ? (
          <div>
            {!result.member_found && (
              <div style={{ background: '#fffbeb', border: '1px solid #fde68a', borderRadius: 8, padding: '0.6rem 0.85rem', fontSize: '0.78rem', color: '#92400e', marginBottom: '0.75rem' }}>
                ⚠ Member not found — using population-average features
              </div>
            )}
            <div style={{ background: isChurn ? '#fef2f2' : '#f0fdf4', border: `1px solid ${isChurn ? '#fecaca' : '#bbf7d0'}`, borderRadius: 12, padding: '1.25rem 1.5rem', display: 'flex', alignItems: 'center', justifyContent: 'space-between' }}>
              <div>
                <div style={{ fontSize: '0.7rem', fontWeight: 700, textTransform: 'uppercase', letterSpacing: '0.1em', color: scoreColor, opacity: 0.7, marginBottom: 4 }}>Churn Probability</div>
                <div style={{ fontSize: '3rem', fontWeight: 800, color: scoreColor, lineHeight: 1, fontVariantNumeric: 'tabular-nums' }}>
                  {(result.churn_probability * 100).toFixed(1)}%
                </div>
                <div style={{ fontSize: '0.75rem', color: scoreColor, opacity: 0.65, marginTop: 6 }}>Threshold: 78.9%</div>
              </div>
              <Badge isChurn={isChurn} />
            </div>
            <div style={{ marginTop: '0.75rem' }}>
              <div style={{ background: '#f3f4f6', borderRadius: 6, height: 8, overflow: 'hidden' }}>
                <div style={{ height: '100%', borderRadius: 6, width: `${result.churn_probability * 100}%`,
                  background: isChurn ? 'linear-gradient(90deg,#fca5a5,#dc2626)' : 'linear-gradient(90deg,#86efac,#16a34a)',
                  transition: 'width 0.6s ease' }} />
              </div>
              <div style={{ position: 'relative', fontSize: '0.7rem', color: '#9ca3af', marginTop: 3, height: 14 }}>
                <span style={{ position: 'absolute', left: 0 }}>0%</span>
                <span style={{ position: 'absolute', left: '78.9%', transform: 'translateX(-50%)', color: '#CF6F3C', whiteSpace: 'nowrap' }}>▲ 78.9%</span>
                <span style={{ position: 'absolute', right: 0 }}>100%</span>
              </div>
            </div>
            <ContributingFactors msno={result.msno} prob={result.churn_probability} shapValues={result.shapValues} />
          </div>
        ) : (
          <div className="empty-state">
            <svg width="42" height="32" viewBox="0 0 42 32" fill="none">
              {[0,6,12,18,24,30,36].map((x, i) => (
                <rect key={i} x={x} y={[10,4,0,6,2,8,12][i]} width="4" height={[12,24,32,20,28,16,8][i]} rx="2" fill="#CF6F3C" fillOpacity="0.18" />
              ))}
            </svg>
            <span style={{ marginTop: 10, fontSize: '0.85rem' }}>Enter a member ID to get a prediction</span>
          </div>
        )}
      </div>
    </div>
  );
}

// ─── Batch Page ───────────────────────────────────────────────────────────────
function BatchPage({ apiUrl = 'http://localhost:8000' }) {
  const [file, setFile] = useState(null);
  const [msnos, setMsnos] = useState(null);
  const [csvError, setCsvError] = useState('');
  const [running, setRunning] = useState(false);
  const [results, setResults] = useState(null);
  const [offline, setOffline] = useState(false);
  const [drag, setDrag] = useState(false);
  const fileRef = useRef();

  const handleFile = async (f) => {
    if (!f) return;
    setFile(f); setResults(null); setCsvError('');
    const text = await f.text();
    const parsed = parseCsv(text);
    if (!parsed) { setCsvError('No "msno" column found in CSV.'); return; }
    if (parsed.length === 0) { setCsvError('CSV has no data rows.'); return; }
    setMsnos(parsed);
  };

  const runBatch = async () => {
    setRunning(true); setOffline(false);
    try {
      // 120s timeout — 5000 users × Redis lookup + inference can take ~60-90s
      const data = await apiPost(apiUrl, '/predict/batch', { msno_list: msnos }, 120000);
      setResults(data); setOffline(false);
    } catch (err) {
      setOffline(err?.message || 'Request failed or timed out');
      setResults(null);
    }
    setRunning(false);
  };

  const churnN   = results ? results.filter(r => r.is_churn).length : 0;
  const retainN  = results ? results.length - churnN : 0;
  const churnRate = results ? churnN / results.length : 0;
  const topRisks = results ? [...results].filter(r => r.is_churn).sort((a, b) => b.churn_probability - a.churn_probability).slice(0, 8) : [];

  return (
    <div>
      <CardTitle>Batch Prediction</CardTitle>
      <p style={{ fontSize: '0.83rem', color: '#6b7280', marginBottom: '1rem' }}>
        Upload a CSV with a column named <code style={{ background: '#f3f4f6', padding: '1px 5px', borderRadius: 4 }}>msno</code>. Results include churn probability and risk label.
      </p>
      {offline && (
        <div style={{ background: '#fef2f2', border: '1px solid #fecaca', borderRadius: 8,
          padding: '0.55rem 0.85rem', fontSize: '0.77rem', color: '#dc2626', marginBottom: '0.9rem' }}>
          ⚠ Batch request failed: {typeof offline === 'string' ? offline : 'unknown error'}. Check the API server or try a smaller file.
        </div>
      )}
      {!file ? (
        <div className={`upload-zone ${drag ? 'drag-over' : ''}`}
          onClick={() => fileRef.current.click()}
          onDragOver={e => { e.preventDefault(); setDrag(true); }}
          onDragLeave={() => setDrag(false)}
          onDrop={e => { e.preventDefault(); setDrag(false); handleFile(e.dataTransfer.files[0]); }}>
          <div style={{ fontSize: '2rem', marginBottom: '0.5rem', opacity: 0.4 }}>📂</div>
          <div style={{ fontWeight: 600, color: '#374151', fontSize: '0.9rem' }}>Drop CSV here or click to browse</div>
          <div style={{ fontSize: '0.78rem', color: '#9ca3af', marginTop: '0.35rem' }}>Requires a column named <code>msno</code></div>
          <input ref={fileRef} type="file" accept=".csv" style={{ display: 'none' }} onChange={e => handleFile(e.target.files[0])} />
        </div>
      ) : (
        <div>
          <div style={{ display: 'flex', alignItems: 'center', gap: '1rem', marginBottom: '0.75rem', padding: '0.75rem 1rem', background: 'white', border: '1px solid #e5e7eb', borderRadius: 10 }}>
            <span style={{ fontSize: '1.4rem' }}>📄</span>
            <div style={{ flex: 1 }}>
              <div style={{ fontWeight: 600, fontSize: '0.875rem', color: '#111827' }}>{file.name}</div>
              <div style={{ fontSize: '0.75rem', color: '#9ca3af' }}>{msnos ? `${msnos.length.toLocaleString()} members` : 'Parsing…'} · {(file.size / 1024).toFixed(1)} KB</div>
            </div>
            <button className="btn btn-secondary" style={{ fontSize: '0.78rem' }} onClick={() => { setFile(null); setMsnos(null); setResults(null); setCsvError(''); }}>✕ Remove</button>
          </div>
          {csvError && <div style={{ color: '#dc2626', fontSize: '0.8rem', marginBottom: '0.5rem' }}>⚠ {csvError}</div>}
          {msnos && !results && (
            <button className="btn btn-primary" onClick={runBatch} disabled={running}>
              {running ? <><Spinner /><span style={{ marginLeft: 8 }}>Running {msnos.length.toLocaleString()} predictions…</span></> : `▶  Run Batch Prediction (${msnos.length.toLocaleString()} members)`}
            </button>
          )}
        </div>
      )}
      {results && (
        <div style={{ marginTop: '1.5rem' }}>
          <div className="stats-grid">
            <StatBox value={results.length.toLocaleString()} label="Total Users" />
            <StatBox value={churnN.toLocaleString()} label="Predicted Churn" variant="danger" />
            <StatBox value={retainN.toLocaleString()} label="Predicted Retain" variant="success" />
            <StatBox value={`${(churnRate * 100).toFixed(1)}%`} label="Churn Rate" variant={churnRate > 0.15 ? 'danger' : 'success'} />
          </div>
          <div className="two-col" style={{ marginTop: '0.5rem' }}>
            <div className="card">
              <CardTitle>Score Distribution</CardTitle>
              <HistogramBars data={results.map(r => r.churn_probability)} />
              <div style={{ display: 'flex', gap: 16, marginTop: '0.5rem', fontSize: '0.75rem' }}>
                <span style={{ display: 'flex', alignItems: 'center', gap: 5 }}><span style={{ width: 10, height: 10, background: '#93c5fd', borderRadius: 2, display: 'inline-block' }} />Low risk</span>
                <span style={{ display: 'flex', alignItems: 'center', gap: 5 }}><span style={{ width: 10, height: 10, background: '#fca5a5', borderRadius: 2, display: 'inline-block' }} />High risk</span>
              </div>
            </div>
            <div className="card">
              <CardTitle>Top Churn Risks</CardTitle>
              {topRisks.length ? (
                <div style={{ overflowY: 'auto', maxHeight: 220 }}>
                  <table className="data-table">
                    <thead><tr><th>Member ID</th><th>Score</th><th>Risk</th></tr></thead>
                    <tbody>
                      {topRisks.map((r, i) => (
                        <tr key={i}>
                          <td style={{ fontFamily: 'monospace', fontSize: '0.75rem' }}>{fmtMsno(r.msno)}</td>
                          <td style={{ color: '#dc2626', fontWeight: 600 }}>{(r.churn_probability * 100).toFixed(1)}%</td>
                          <td><span className="badge churn" style={{ fontSize: '0.72rem', padding: '2px 10px' }}>⚠ Churn</span></td>
                        </tr>
                      ))}
                    </tbody>
                  </table>
                </div>
              ) : <div style={{ color: '#9ca3af', fontSize: '0.85rem' }}>No high-risk users found.</div>}
            </div>
          </div>
          <button className="btn btn-secondary" style={{ marginTop: '1rem' }}
            onClick={() => {
              const csv = ['msno,churn_probability,is_churn', ...results.map(r => `${r.msno},${r.churn_probability.toFixed(4)},${r.is_churn}`)].join('\n');
              const a = document.createElement('a');
              a.href = 'data:text/csv;charset=utf-8,' + encodeURIComponent(csv);
              a.download = 'churn_predictions.csv'; a.click();
            }}>
            ⬇ Download results (CSV)
          </button>
        </div>
      )}
    </div>
  );
}

// ─── Statistics Page ──────────────────────────────────────────────────────────
function StatisticsPage({ apiUrl = 'http://localhost:8000' }) {
  const [stats, setStats] = useState(MOCK_STATS);
  const [churned, setChurned] = useState(MOCK_CHURNED);
  const [offline, setOffline] = useState(false);
  const [lastUpdated, setLastUpdated] = useState(null);
  const [age, setAge] = useState(0);
  const [search, setSearch] = useState('');

  const fetchData = async () => {
    try {
      const [s, c] = await Promise.all([
        apiGet(apiUrl, '/stats'),
        apiGet(apiUrl, '/stats/churned?limit=500'),
      ]);
      setStats({ total_predictions: s.total_predictions, churn_count: s.churn_count, retain_count: s.retain_count, churn_rate: s.churn_rate });
      setChurned(c.churned || []);
      setOffline(false);
    } catch {
      setStats(MOCK_STATS); setChurned(MOCK_CHURNED); setOffline(true);
    }
    setLastUpdated(Date.now());
  };

  useEffect(() => {
    fetchData();
    const id = setInterval(fetchData, 10000);
    return () => clearInterval(id);
  }, [apiUrl]);

  useEffect(() => {
    const id = setInterval(() => { if (lastUpdated) setAge(Math.floor((Date.now() - lastUpdated) / 1000)); }, 1000);
    return () => clearInterval(id);
  }, [lastUpdated]);

  const filtered = churned.filter(u => u.msno.toLowerCase().includes(search.toLowerCase()));

  return (
    <div>
      {offline && <OfflineBanner />}
      <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', marginBottom: '1rem' }}>
        <CardTitle>Prediction Statistics</CardTitle>
        <div style={{ display: 'flex', alignItems: 'center', gap: 8, fontSize: '0.75rem', color: '#9ca3af' }}>
          <span style={{ width: 7, height: 7, borderRadius: '50%', background: offline ? '#f87171' : '#4ade80',
            boxShadow: offline ? 'none' : '0 0 6px rgba(74,222,128,0.6)', display: 'inline-block',
            animation: offline ? 'none' : 'pulse 2s infinite' }} />
          {lastUpdated ? `Updated ${age}s ago` : 'Loading…'} · auto-refreshes every 10s
          <button className="btn btn-secondary" style={{ fontSize: '0.72rem', padding: '0.3rem 0.7rem' }} onClick={fetchData}>↻</button>
        </div>
      </div>
      <div className="stats-grid">
        <StatBox value={stats.total_predictions.toLocaleString()} label="Total Predictions" />
        <StatBox value={stats.churn_count.toLocaleString()} label="Churn" variant="danger" />
        <StatBox value={stats.retain_count.toLocaleString()} label="Retain" variant="success" />
        <StatBox value={`${(stats.churn_rate * 100).toFixed(1)}%`} label="Churn Rate" variant={stats.churn_rate > 0.15 ? 'danger' : 'success'} />
      </div>
      <div className="two-col" style={{ marginTop: '0.5rem', alignItems: 'start' }}>
        <div className="card">
          <CardTitle>Distribution</CardTitle>
          <DonutChart churnN={stats.churn_count} retainN={stats.retain_count} />
        </div>
        <div className="card">
          <CardTitle>Summary</CardTitle>
          <table style={{ width: '100%', borderCollapse: 'collapse', fontSize: '0.85rem' }}>
            <tbody>
              {[
                ['Total predictions', stats.total_predictions.toLocaleString()],
                ['Predicted to churn', `${stats.churn_count.toLocaleString()} (${(stats.churn_rate * 100).toFixed(1)}%)`],
                ['Predicted to retain', `${stats.retain_count.toLocaleString()} (${((1 - stats.churn_rate) * 100).toFixed(1)}%)`],
                ['Decision threshold', '78.9%'],
                ['Model', 'XGBoost · AUC-ROC 0.8924'],
              ].map(([k, v]) => (
                <tr key={k} style={{ borderBottom: '1px solid #f3f4f6' }}>
                  <td style={{ padding: '0.6rem 0.5rem', color: '#6b7280', fontWeight: 500 }}>{k}</td>
                  <td style={{ padding: '0.6rem 0.5rem', color: '#111827', fontWeight: 600, textAlign: 'right' }}>{v}</td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      </div>
      <div className="card" style={{ marginTop: '1rem' }}>
        <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', marginBottom: '0.75rem' }}>
          <CardTitle>Predicted Churn Users</CardTitle>
          <input className="input" style={{ width: 220, fontSize: '0.78rem' }} value={search}
            onChange={e => setSearch(e.target.value)} placeholder="🔍 Filter by member ID…" />
        </div>
        <p style={{ fontSize: '0.75rem', color: '#9ca3af', marginBottom: '0.5rem' }}>{filtered.length} users predicted to churn</p>
        <div style={{ overflowY: 'auto', maxHeight: 260 }}>
          <table className="data-table">
            <thead><tr><th>Member ID</th><th>Score</th><th>Predicted At</th></tr></thead>
            <tbody>
              {filtered.map((u, i) => (
                <tr key={i}>
                  <td style={{ fontFamily: 'monospace', fontSize: '0.75rem' }}>{fmtMsno(u.msno)}</td>
                  <td style={{ color: '#dc2626', fontWeight: 600 }}>{(u.churn_probability * 100).toFixed(1)}%</td>
                  <td style={{ color: '#9ca3af' }}>{u.predicted_at}</td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      </div>
    </div>
  );
}

// ─── Model Info Page ──────────────────────────────────────────────────────────
function ModelInfoPage() {
  const [tab, setTab] = useState('overview');
  const metrics = [
    { label: 'AUC-ROC', value: '0.8924', note: 'Primary metric' },
    { label: 'AUC-PR', value: '0.5044', note: 'Imbalanced classes' },
    { label: 'F1 Score', value: '0.5068', note: 'Threshold = 0.789' },
    { label: 'Precision', value: '0.3593', note: '35.9% of predicted churn are true' },
    { label: 'Recall', value: '0.8596', note: '86.0% of churners caught' },
    { label: 'Threshold', value: '0.789', note: 'F1-optimal cut-off' },
  ];
  return (
    <div>
      <div className="card" style={{ marginBottom: '1.25rem', display: 'flex', alignItems: 'center', gap: '1.25rem' }}>
        <div style={{ width: 48, height: 48, borderRadius: 12, background: 'linear-gradient(135deg,#CF6F3C,#e8954a)', display: 'flex', alignItems: 'center', justifyContent: 'center', fontSize: '1.5rem', flexShrink: 0 }}>🤖</div>
        <div style={{ flex: 1 }}>
          <div style={{ fontWeight: 700, fontSize: '1rem', color: '#111827' }}>kkbox-churn-model · v1.0</div>
          <div style={{ fontSize: '0.8rem', color: '#6b7280', marginTop: 2 }}>XGBoost Classifier · Trained on 804k rows · Out-of-time split at 2016-06-01</div>
        </div>
        <span className="badge retain">✓ Production</span>
      </div>
      <div style={{ display: 'flex', gap: 4, marginBottom: '1.25rem', borderBottom: '1px solid #e5e7eb' }}>
        {[['overview','Overview'],['roc','ROC Curve'],['features','Features']].map(([id, label]) => (
          <button key={id} onClick={() => setTab(id)} style={{ padding: '0.5rem 1rem', border: 'none', background: 'none', cursor: 'pointer', fontSize: '0.875rem', fontWeight: tab === id ? 600 : 400, color: tab === id ? '#CF6F3C' : '#6b7280', borderBottom: tab === id ? '2px solid #CF6F3C' : '2px solid transparent', fontFamily: 'Inter,sans-serif', marginBottom: -1 }}>{label}</button>
        ))}
      </div>
      {tab === 'overview' && (
        <div>
          <div style={{ display: 'grid', gridTemplateColumns: 'repeat(3,1fr)', gap: '1rem', marginBottom: '1rem' }}>
            {metrics.map(m => (
              <div key={m.label} className="metric-card">
                <div className="metric-label">{m.label}</div>
                <div className="metric-value">{m.value}</div>
                <div style={{ fontSize: '0.72rem', color: '#9ca3af', marginTop: 4 }}>{m.note}</div>
              </div>
            ))}
          </div>
          <div className="card" style={{ fontSize: '0.83rem', color: '#374151', lineHeight: 1.7 }}>
            <CardTitle>Training Notes</CardTitle>
            <p>Split strategy: <strong>out-of-time</strong> — train on members with <code style={{ background: '#f3f4f6', padding: '1px 4px', borderRadius: 3 }}>registration_init_time &lt; 2016-06-01</code>.</p>
            <p style={{ marginTop: '0.5rem' }}>Train: <strong>804k rows</strong> · Test: <strong>157k rows</strong> · Base churn rate: <strong>~10%</strong> · Features sourced from Feast feature store (BigQuery offline / Redis online).</p>
          </div>
        </div>
      )}
      {tab === 'roc' && (
        <div className="two-col" style={{ alignItems: 'start' }}>
          <div className="card"><CardTitle>ROC Curve</CardTitle><ROCCurve /></div>
          <div className="card">
            <CardTitle>Interpretation</CardTitle>
            <p style={{ fontSize: '0.83rem', color: '#374151', lineHeight: 1.7 }}>An AUC-ROC of <strong style={{ color: '#CF6F3C' }}>0.8924</strong> means the model correctly ranks a randomly selected churner above a randomly selected retainer ~89% of the time.</p>
            <p style={{ fontSize: '0.83rem', color: '#374151', lineHeight: 1.7, marginTop: '0.75rem' }}>The optimal threshold of <strong>78.9%</strong> was chosen to maximise F1 score on the held-out test set.</p>
          </div>
        </div>
      )}
      {tab === 'features' && (
        <div className="card"><CardTitle>Feature Importance (Gain)</CardTitle><FeatureImportanceChart /></div>
      )}
    </div>
  );
}

// ─── API Health Page ──────────────────────────────────────────────────────────
function APIHealthPage({ apiUrl = 'http://localhost:8000' }) {
  const [apiStatus, setApiStatus] = useState('checking');
  const [apiLatency, setApiLatency] = useState(null);
  const [lastChecked, setLastChecked] = useState('—');

  const recheck = async () => {
    setApiStatus('checking');
    const t0 = performance.now();
    try {
      await apiGet(apiUrl, '/health');
      setApiStatus('online');
      setApiLatency(Math.round(performance.now() - t0));
    } catch {
      setApiStatus('offline');
      setApiLatency(null);
    }
    setLastChecked(new Date().toLocaleTimeString());
  };

  useEffect(() => { recheck(); const id = setInterval(recheck, 15000); return () => clearInterval(id); }, [apiUrl]);

  const services = [
    { name: 'FastAPI Serving',       url: apiUrl,          status: apiStatus, latency: apiLatency ? `${apiLatency}ms` : '—', icon: '⚡', note: 'Direct health check' },
    { name: 'Redis (Feature Store)', url: '10.80.68.19:6379', status: 'internal', latency: '~3ms', icon: '🗄', note: 'Internal — not reachable from browser' },
    { name: 'MLflow Tracking',       url: 'localhost:5000',  status: 'internal', latency: '~42ms', icon: '🧪', note: 'Internal — not reachable from browser' },
    { name: 'Prometheus',            url: 'localhost:9090',  status: 'internal', latency: '~12ms', icon: '📊', note: 'Internal — not reachable from browser' },
  ];

  const endpoints = [
    { method: 'POST', path: '/predict/',       desc: 'Single-user prediction',  latency: '22ms' },
    { method: 'POST', path: '/predict/batch',  desc: 'Batch prediction',         latency: '890ms' },
    { method: 'POST', path: '/explain',        desc: 'SHAP feature explanation', latency: '45ms' },
    { method: 'GET',  path: '/health',         desc: 'Health check',             latency: '1ms' },
    { method: 'GET',  path: '/stats',          desc: 'Prediction statistics',    latency: '5ms' },
    { method: 'GET',  path: '/metrics',        desc: 'Prometheus scrape target', latency: '4ms' },
    { method: 'GET',  path: '/sample',         desc: 'Random msno sample',       latency: '31ms' },
  ];

  return (
    <div>
      <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', marginBottom: '1rem' }}>
        <CardTitle>Service Health</CardTitle>
        <div style={{ display: 'flex', alignItems: 'center', gap: '0.75rem' }}>
          <span style={{ fontSize: '0.75rem', color: '#9ca3af' }}>Last checked: {lastChecked}</span>
          <button className="btn btn-secondary" style={{ fontSize: '0.78rem' }} onClick={recheck}>↻ Re-check</button>
        </div>
      </div>
      <div style={{ display: 'grid', gridTemplateColumns: 'repeat(2,1fr)', gap: '1rem', marginBottom: '1.5rem' }}>
        {services.map(s => (
          <div key={s.name} className="service-card">
            <div style={{ fontSize: '1.6rem' }}>{s.icon}</div>
            <div style={{ flex: 1 }}>
              <div style={{ fontWeight: 600, fontSize: '0.875rem', color: '#111827' }}>{s.name}</div>
              <div style={{ fontSize: '0.72rem', color: '#9ca3af', fontFamily: 'monospace' }}>{s.url}</div>
              <div style={{ fontSize: '0.7rem', color: '#c4c8ce', marginTop: 2 }}>{s.note}</div>
            </div>
            <div style={{ textAlign: 'right', flexShrink: 0 }}>
              <div style={{ display: 'flex', alignItems: 'center', gap: 6, justifyContent: 'flex-end' }}>
                {s.status !== 'internal' && (
                  <div className={`status-dot ${s.status === 'online' ? 'online' : s.status === 'offline' ? 'offline' : ''}`}
                    style={s.status === 'checking' ? { background: '#fbbf24', boxShadow: '0 0 6px rgba(251,191,36,0.5)' } : {}} />
                )}
                <span style={{ fontSize: '0.8rem', fontWeight: 600, color: s.status === 'online' ? '#16a34a' : s.status === 'offline' ? '#dc2626' : s.status === 'checking' ? '#d97706' : '#9ca3af' }}>
                  {s.status === 'online' ? 'Online' : s.status === 'offline' ? 'Offline' : s.status === 'checking' ? 'Checking…' : 'Internal'}
                </span>
              </div>
              {s.latency && s.latency !== '—' && <div style={{ fontSize: '0.72rem', color: '#9ca3af', marginTop: 2 }}>Avg {s.latency}</div>}
            </div>
          </div>
        ))}
      </div>
      <div className="stats-grid" style={{ marginBottom: '1.5rem' }}>
        <StatBox value="1,247" label="Requests Today" />
        <StatBox value={apiLatency ? `${apiLatency}ms` : '—'} label="Last Latency" variant={apiLatency && apiLatency < 100 ? 'success' : ''} />
        <StatBox value="0.02%" label="Error Rate" variant="success" />
        <StatBox value="99.9%" label="Uptime (30d)" variant="success" />
      </div>
      <div className="card">
        <CardTitle>API Endpoints</CardTitle>
        <table className="data-table">
          <thead><tr><th>Method</th><th>Path</th><th>Description</th><th>Avg Latency</th></tr></thead>
          <tbody>
            {endpoints.map(ep => (
              <tr key={ep.path}>
                <td><span style={{ padding: '2px 8px', borderRadius: 4, fontSize: '0.72rem', fontWeight: 700, fontFamily: 'monospace', background: ep.method === 'POST' ? '#eff6ff' : '#f0fdf4', color: ep.method === 'POST' ? '#2563eb' : '#16a34a' }}>{ep.method}</span></td>
                <td style={{ fontFamily: 'monospace', fontSize: '0.8rem', color: '#374151' }}>{ep.path}</td>
                <td style={{ color: '#6b7280' }}>{ep.desc}</td>
                <td style={{ color: '#9ca3af', fontFamily: 'monospace' }}>{ep.latency}</td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>
    </div>
  );
}

// ─── Streaming Simulation Page ────────────────────────────────────────────────
function StreamingPage({ apiUrl = '', onSelectMsno }) {
  const [streamStatus, setStreamStatus] = useState('idle');
  const [currentDate, setCurrentDate] = useState(null);
  const [datesDone, setDatesDone] = useState(0);
  const [datesList, setDatesList] = useState([]);
  const [speed, setSpeed] = useState(55);
  const [selectedDate, setSelectedDate] = useState(null);
  const [usersData, setUsersData] = useState(null);
  const [userPage, setUserPage] = useState(1);
  const [loadingUsers, setLoadingUsers] = useState(false);
  const esRef = useRef(null);
  const TOTAL_DAYS = 31;

  // SSE subscription
  useEffect(() => {
    const es = new EventSource(`${apiUrl}/stream/events`);
    esRef.current = es;
    es.addEventListener('status', e => {
      const d = JSON.parse(e.data);
      if (d.status) setStreamStatus(d.status);
      if (d.current_date) setCurrentDate(d.current_date);
      if (d.dates_done !== undefined) setDatesDone(d.dates_done);
    });
    es.addEventListener('date_done', e => {
      const d = JSON.parse(e.data);
      setCurrentDate(d.date);
      setDatesDone(d.dates_done);
      setDatesList(prev => [...new Set([...prev, d.date])].sort());
      setSelectedDate(d.date);
      setUserPage(1);
    });
    return () => es.close();
  }, [apiUrl]);

  // Load users when date or page changes
  useEffect(() => {
    if (!selectedDate) return;
    setLoadingUsers(true);
    apiGet(apiUrl, `/stream/users?date=${selectedDate}&page=${userPage}&limit=20`)
      .then(d => setUsersData(d))
      .catch(() => setUsersData(null))
      .finally(() => setLoadingUsers(false));
  }, [selectedDate, userPage, apiUrl]);

  const handleStart = () => {
    setDatesList([]); setDatesDone(0); setCurrentDate(null);
    setSelectedDate(null); setUsersData(null);
    apiPost(apiUrl, '/stream/start', { speed }).catch(() => {});
  };
  const handlePause  = () => apiPost(apiUrl, '/stream/pause',  {}).catch(() => {});
  const handleResume = () => apiPost(apiUrl, '/stream/resume', {}).catch(() => {});
  const handleStop   = () => apiPost(apiUrl, '/stream/stop',   {}).catch(() => {});

  const isIdle    = streamStatus === 'idle';
  const isRunning = streamStatus === 'running';
  const isPaused  = streamStatus === 'paused';
  const isDone    = streamStatus === 'done';
  const isActive  = isRunning || isPaused;

  const progress = Math.min(datesDone / TOTAL_DAYS, 1);
  const statusColor = { idle: '#9ca3af', running: '#16a34a', paused: '#d97706', done: '#CF6F3C', error: '#dc2626' }[streamStatus] || '#9ca3af';
  const statusLabel = { idle: 'Idle', running: 'Streaming…', paused: 'Paused', done: 'Complete', error: 'Error' }[streamStatus] || '';

  return (
    <div>
      {/* Control Panel */}
      <div className="card" style={{ marginBottom: '1.25rem' }}>
        <CardTitle>Historical Playback — March 2017</CardTitle>

        <div style={{ display: 'flex', alignItems: 'center', gap: '1.25rem', marginBottom: '0.9rem', flexWrap: 'wrap' }}>
          <div style={{ display: 'flex', alignItems: 'center', gap: 7 }}>
            <div style={{ width: 9, height: 9, borderRadius: '50%', background: statusColor, flexShrink: 0,
              boxShadow: isRunning ? `0 0 7px ${statusColor}` : 'none',
              animation: isRunning ? 'pulse 2s infinite' : 'none' }} />
            <span style={{ fontSize: '0.85rem', fontWeight: 600, color: statusColor }}>{statusLabel}</span>
          </div>
          {currentDate && (
            <div style={{ fontSize: '1.2rem', fontWeight: 700, color: '#111827', fontVariantNumeric: 'tabular-nums' }}>
              📅 {currentDate}
            </div>
          )}
          {datesDone > 0 && (
            <span style={{ fontSize: '0.8rem', color: '#9ca3af' }}>Day {datesDone} / {TOTAL_DAYS}</span>
          )}
        </div>

        {/* Progress bar */}
        <div style={{ background: '#f3f4f6', borderRadius: 6, height: 8, overflow: 'hidden', marginBottom: '1rem' }}>
          <div style={{ height: '100%', borderRadius: 6, width: `${progress * 100}%`,
            background: isDone ? 'linear-gradient(90deg,#CF6F3C,#e8954a)' : 'linear-gradient(90deg,#86efac,#16a34a)',
            transition: 'width 0.6s ease' }} />
        </div>

        {/* Buttons */}
        <div style={{ display: 'flex', alignItems: 'center', gap: '0.75rem', flexWrap: 'wrap' }}>
          {(isIdle || isDone) && (
            <button className="btn btn-primary" onClick={handleStart}>▶ {isDone ? 'Restart' : 'Start Simulation'}</button>
          )}
          {isRunning && <button className="btn btn-secondary" onClick={handlePause}>⏸ Pause</button>}
          {isPaused  && <button className="btn btn-primary"   onClick={handleResume}>▶ Resume</button>}
          {isActive  && (
            <button className="btn btn-secondary" style={{ color: '#dc2626' }} onClick={handleStop}>⏹ Stop</button>
          )}

          {isIdle && !isDone && (
            <div style={{ display: 'flex', alignItems: 'center', gap: 8, marginLeft: 'auto', fontSize: '0.8rem', color: '#6b7280' }}>
              <span>Speed:</span>
              <select className="input" style={{ width: 'auto', padding: '0.3rem 0.6rem', fontSize: '0.8rem' }}
                value={speed} onChange={e => setSpeed(Number(e.target.value))}>
                <option value={5}>5s / day (fast)</option>
                <option value={30}>30s / day</option>
                <option value={55}>55s / day (demo)</option>
                <option value={120}>2min / day (slow)</option>
              </select>
            </div>
          )}
          {isActive && (
            <span style={{ marginLeft: 'auto', fontSize: '0.75rem', color: '#9ca3af' }}>
              {speed}s / day · pause anytime to interact
            </span>
          )}
        </div>

        {isIdle && !isDone && (
          <p style={{ fontSize: '0.78rem', color: '#9ca3af', marginTop: '0.85rem', lineHeight: 1.65 }}>
            Replays KKBox streaming data from March 2017 through Kafka → BigQuery → Feast → Redis.
            Each day a new batch of users appears below. Click <strong>Predict →</strong> on any user
            to jump to the Single User page and run a churn prediction.
          </p>
        )}
      </div>

      {/* Date chips + User list */}
      {datesList.length > 0 && (
        <div className="two-col" style={{ alignItems: 'start' }}>

          {/* Date chips */}
          <div className="card">
            <div className="card-title">Processed Dates ({datesList.length})</div>
            <div style={{ display: 'flex', flexWrap: 'wrap', gap: '0.4rem' }}>
              {datesList.map(d => (
                <button key={d}
                  onClick={() => { setSelectedDate(d); setUserPage(1); }}
                  style={{ padding: '0.3rem 0.75rem', borderRadius: 6, border: '1px solid',
                    borderColor: selectedDate === d ? '#CF6F3C' : '#e5e7eb',
                    background: selectedDate === d ? 'rgba(207,111,60,0.09)' : 'white',
                    color: selectedDate === d ? '#CF6F3C' : '#374151',
                    fontSize: '0.8rem', fontWeight: selectedDate === d ? 600 : 400,
                    cursor: 'pointer', fontFamily: 'Inter,sans-serif' }}>
                  {d}
                </button>
              ))}
            </div>
          </div>

          {/* User list */}
          <div className="card">
            <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'baseline', marginBottom: '0.75rem' }}>
              <div className="card-title" style={{ margin: 0 }}>
                Users — {selectedDate || '—'}
                {usersData ? ` (${usersData.total.toLocaleString()})` : ''}
              </div>
            </div>

            {loadingUsers ? (
              <div style={{ display: 'flex', justifyContent: 'center', padding: '2rem' }}><Spinner /></div>
            ) : usersData && usersData.users.length > 0 ? (
              <>
                <div style={{ overflowY: 'auto', maxHeight: 320 }}>
                  <table className="data-table">
                    <thead><tr><th>Member ID</th><th>Action</th></tr></thead>
                    <tbody>
                      {usersData.users.map((msno, i) => (
                        <tr key={i}>
                          <td style={{ fontFamily: 'monospace', fontSize: '0.75rem' }}>{fmtMsno(msno)}</td>
                          <td>
                            <button onClick={() => onSelectMsno && onSelectMsno(msno)}
                              style={{ padding: '0.25rem 0.75rem', borderRadius: 6,
                                border: '1px solid #CF6F3C', background: 'rgba(207,111,60,0.06)',
                                color: '#CF6F3C', fontSize: '0.75rem', fontWeight: 600,
                                cursor: 'pointer', fontFamily: 'Inter,sans-serif' }}>
                              Predict →
                            </button>
                          </td>
                        </tr>
                      ))}
                    </tbody>
                  </table>
                </div>
                {usersData.pages > 1 && (
                  <div style={{ display: 'flex', justifyContent: 'center', alignItems: 'center', gap: 6, marginTop: '0.75rem' }}>
                    <button className="btn btn-secondary" style={{ padding: '0.3rem 0.75rem', fontSize: '0.78rem' }}
                      disabled={userPage === 1} onClick={() => setUserPage(p => p - 1)}>‹ Prev</button>
                    <span style={{ fontSize: '0.78rem', color: '#6b7280' }}>{userPage} / {usersData.pages}</span>
                    <button className="btn btn-secondary" style={{ padding: '0.3rem 0.75rem', fontSize: '0.78rem' }}
                      disabled={userPage === usersData.pages} onClick={() => setUserPage(p => p + 1)}>Next ›</button>
                  </div>
                )}
              </>
            ) : selectedDate ? (
              <div className="empty-state" style={{ minHeight: 120 }}>
                <span style={{ fontSize: '0.82rem' }}>
                  {usersData && usersData.total === 0 ? 'No users found for this date' : 'Waiting for data…'}
                </span>
              </div>
            ) : null}
          </div>

        </div>
      )}
    </div>
  );
}

Object.assign(window, { SingleUserPage, BatchPage, StatisticsPage, ModelInfoPage, APIHealthPage, StreamingPage });
