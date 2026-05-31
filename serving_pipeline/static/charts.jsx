// charts.jsx — SVG chart components for KKBox Dashboard
// Exported to window: GaugeChart, DonutChart, HistogramBars, ROCCurve, FeatureImportanceChart

const THRESHOLD = 0.789;
const OG = '#CF6F3C';

// ─── Gauge Chart ──────────────────────────────────────────────────────────────
function GaugeChart({ value = 0, threshold = THRESHOLD, loading = false }) {
  const cx = 120, cy = 108, r = 84, sw = 15;
  const pct = Math.min(Math.max(value, 0), 0.9999);
  const isHigh = pct >= threshold;
  const color = isHigh ? '#dc2626' : '#16a34a';

  const pt = (deg) => {
    const rad = (deg * Math.PI) / 180;
    return { x: +(cx + r * Math.cos(rad)).toFixed(2), y: +(cy - r * Math.sin(rad)).toFixed(2) };
  };

  const s = pt(180), e = pt(0);
  const vp = pt(180 - pct * 180);
  const bgD  = `M ${s.x} ${s.y} A ${r} ${r} 0 0 0 ${e.x} ${e.y}`;
  const valD = `M ${s.x} ${s.y} A ${r} ${r} 0 0 0 ${vp.x} ${vp.y}`;

  const tDeg  = 180 - threshold * 180;
  const tCos  = Math.cos((tDeg * Math.PI) / 180);
  const tSin  = Math.sin((tDeg * Math.PI) / 180);
  const tInner  = { x: +(cx + (r - 13) * tCos).toFixed(1), y: +(cy - (r - 13) * tSin).toFixed(1) };
  const tOuter  = { x: +(cx + (r + 13) * tCos).toFixed(1), y: +(cy - (r + 13) * tSin).toFixed(1) };
  const tLabel  = { x: +(cx + (r + 26) * tCos).toFixed(1), y: +(cy - (r + 26) * tSin).toFixed(1) };

  return (
    <svg viewBox="0 0 240 128" style={{ width: '100%', maxWidth: 260, display: 'block', margin: '0 auto' }}>
      {/* Background zones */}
      <path d={bgD} fill="none" stroke="#f0efec" strokeWidth={sw} strokeLinecap="round" />
      {/* Value arc */}
      {pct > 0.005 && !loading && (
        <path d={valD} fill="none" stroke={color} strokeWidth={sw} strokeLinecap="round"
          style={{ transition: 'all 0.7s cubic-bezier(0.4,0,0.2,1)' }} />
      )}
      {/* Threshold tick */}
      <line x1={tInner.x} y1={tInner.y} x2={tOuter.x} y2={tOuter.y} stroke="#9ca3af" strokeWidth="2.5" />
      <text x={tLabel.x} y={tLabel.y + 3} textAnchor="middle" fontSize="9" fill="#9ca3af"
        style={{ fontFamily: 'Inter,sans-serif' }}>78.9%</text>
      {/* Center value */}
      <text x={cx} y={cy - 8} textAnchor="middle" fontSize="34" fontWeight="700"
        fill={loading ? '#d1d5db' : color} style={{ fontFamily: 'Inter,sans-serif', transition: 'fill 0.4s' }}>
        {loading ? '···' : `${(pct * 100).toFixed(1)}%`}
      </text>
      <text x={cx} y={cy + 14} textAnchor="middle" fontSize="11" fill="#9ca3af"
        style={{ fontFamily: 'Inter,sans-serif' }}>churn probability</text>
      {/* Axis labels */}
      <text x={s.x - 4} y={cy + 20} textAnchor="end" fontSize="9" fill="#d1d5db">0%</text>
      <text x={e.x + 4} y={cy + 20} textAnchor="start" fontSize="9" fill="#d1d5db">100%</text>
    </svg>
  );
}

// ─── Donut Chart ──────────────────────────────────────────────────────────────
function DonutChart({ churnN = 0, retainN = 0 }) {
  const total = churnN + retainN;
  if (!total) return <div style={{ height: 170 }} />;
  const cx = 80, cy = 80, or = 62, ir = 44;
  const cp = churnN / total;

  const arc = (from, to) => {
    if (to - from >= 0.9999) {
      return `M ${cx + or} ${cy} A ${or} ${or} 0 1 1 ${cx - or} ${cy} A ${or} ${or} 0 1 1 ${cx + or} ${cy}`;
    }
    const p = (pct, radius) => {
      const a = pct * 2 * Math.PI - Math.PI / 2;
      return { x: +(cx + radius * Math.cos(a)).toFixed(2), y: +(cy + radius * Math.sin(a)).toFixed(2) };
    };
    const s = p(from, or), e = p(to, or);
    const si = p(to, ir), ei = p(from, ir);
    const lg = (to - from) > 0.5 ? 1 : 0;
    return `M ${s.x} ${s.y} A ${or} ${or} 0 ${lg} 1 ${e.x} ${e.y} L ${si.x} ${si.y} A ${ir} ${ir} 0 ${lg} 0 ${ei.x} ${ei.y} Z`;
  };

  return (
    <svg viewBox="0 0 160 170" style={{ width: '100%', maxWidth: 180 }}>
      {cp > 0.001 && <path d={arc(0, cp)} fill="#fca5a5" />}
      {(1 - cp) > 0.001 && <path d={arc(cp, 1)} fill="#86efac" />}
      <text x={cx} y={cy - 4} textAnchor="middle" fontSize="19" fontWeight="700" fill="#111827"
        style={{ fontFamily: 'Inter,sans-serif' }}>{(cp * 100).toFixed(1)}%</text>
      <text x={cx} y={cy + 14} textAnchor="middle" fontSize="10" fill="#9ca3af"
        style={{ fontFamily: 'Inter,sans-serif' }}>churn rate</text>
      <circle cx="16" cy="153" r="5" fill="#fca5a5" />
      <text x="25" y="157" fontSize="11" fill="#374151" style={{ fontFamily: 'Inter,sans-serif' }}>
        Churn ({churnN.toLocaleString()})
      </text>
      <circle cx="16" cy="168" r="5" fill="#86efac" />
      <text x="25" y="172" fontSize="11" fill="#374151" style={{ fontFamily: 'Inter,sans-serif' }}>
        Retain ({retainN.toLocaleString()})
      </text>
    </svg>
  );
}

// ─── Histogram Bars ───────────────────────────────────────────────────────────
function HistogramBars({ data = [], threshold = THRESHOLD }) {
  const BINS = 20;
  const counts = new Array(BINS).fill(0);
  data.forEach(p => counts[Math.min(Math.floor(p * BINS), BINS - 1)]++);
  const max = Math.max(...counts, 1);
  const W = 340, H = 155, PL = 32, PR = 12, PT = 12, PB = 30;
  const iW = W - PL - PR, iH = H - PT - PB;
  const bW = iW / BINS - 1.5;

  return (
    <svg viewBox={`0 0 ${W} ${H}`} style={{ width: '100%' }}>
      {[0.25, 0.5, 0.75, 1].map(v => (
        <line key={v} x1={PL} y1={PT + iH - v * iH} x2={PL + iW} y2={PT + iH - v * iH}
          stroke="#f3f4f6" strokeWidth="1" />
      ))}
      {counts.map((c, i) => {
        const x = PL + i * (iW / BINS);
        const bh = Math.max((c / max) * iH, c > 0 ? 2 : 0);
        const y = PT + iH - bh;
        const mid = (i + 0.5) / BINS;
        return <rect key={i} x={x} y={y} width={bW} height={bh}
          fill={mid >= threshold ? '#fca5a5' : '#93c5fd'} rx="1.5" />;
      })}
      {/* Threshold line */}
      {(() => {
        const tx = PL + threshold * iW;
        return (
          <>
            <line x1={tx} y1={PT} x2={tx} y2={PT + iH} stroke={OG} strokeWidth="2" strokeDasharray="4,3" />
            <text x={tx + 3} y={PT + 11} fontSize="9" fill={OG} style={{ fontFamily: 'Inter,sans-serif' }}>threshold</text>
          </>
        );
      })()}
      {[0, 25, 50, 75, 100].map(v => (
        <text key={v} x={PL + (v / 100) * iW} y={H - 4} textAnchor="middle" fontSize="9" fill="#9ca3af">
          {v}%
        </text>
      ))}
      <text x={PL + iW / 2} y={H} textAnchor="middle" fontSize="9" fill="#c4c8ce">Score</text>
    </svg>
  );
}

// ─── ROC Curve ────────────────────────────────────────────────────────────────
function ROCCurve() {
  const pts = [
    [0,0],[0.01,0.10],[0.03,0.25],[0.05,0.38],[0.08,0.50],
    [0.12,0.60],[0.17,0.69],[0.22,0.76],[0.28,0.82],[0.35,0.87],
    [0.43,0.91],[0.52,0.94],[0.62,0.96],[0.73,0.98],[0.85,0.99],[1,1]
  ];
  const W = 280, H = 210, PL = 38, PR = 16, PT = 16, PB = 36;
  const iW = W - PL - PR, iH = H - PT - PB;
  const px = x => PL + x * iW, py = y => PT + (1 - y) * iH;
  const pathD = pts.map(([x, y], i) => `${i ? 'L' : 'M'} ${px(x).toFixed(1)} ${py(y).toFixed(1)}`).join(' ');

  return (
    <svg viewBox={`0 0 ${W} ${H}`} style={{ width: '100%', maxWidth: 280 }}>
      {[0, 0.25, 0.5, 0.75, 1].map(v => (
        <React.Fragment key={v}>
          <line x1={px(0)} y1={py(v)} x2={px(1)} y2={py(v)} stroke="#f3f4f6" strokeWidth="1" />
          <line x1={px(v)} y1={py(0)} x2={px(v)} y2={py(1)} stroke="#f3f4f6" strokeWidth="1" />
        </React.Fragment>
      ))}
      <line x1={px(0)} y1={py(0)} x2={px(1)} y2={py(1)} stroke="#e5e7eb" strokeWidth="1.5" strokeDasharray="5,3" />
      <path d={`${pathD} L ${px(1)} ${py(0)} L ${px(0)} ${py(0)} Z`} fill={OG} fillOpacity="0.08" />
      <path d={pathD} fill="none" stroke={OG} strokeWidth="2.5" strokeLinejoin="round" />
      <rect x={px(0.56)} y={py(0.22)} width={80} height={22} rx="5" fill="white" stroke="#f0efec" />
      <text x={px(0.56) + 40} y={py(0.22) + 14} textAnchor="middle" fontSize="11" fill={OG} fontWeight="600"
        style={{ fontFamily: 'Inter,sans-serif' }}>AUC = 0.8924</text>
      {[0, 0.5, 1].map(v => (
        <React.Fragment key={v}>
          <text x={px(v)} y={H - 6} textAnchor="middle" fontSize="9" fill="#9ca3af">{v}</text>
          <text x={PL - 5} y={py(v) + 3} textAnchor="end" fontSize="9" fill="#9ca3af">{v}</text>
        </React.Fragment>
      ))}
      <text x={px(0.5)} y={H - 1} textAnchor="middle" fontSize="10" fill="#6b7280"
        style={{ fontFamily: 'Inter,sans-serif' }}>False Positive Rate</text>
      <text x={10} y={py(0.5)} textAnchor="middle" fontSize="10" fill="#6b7280"
        transform={`rotate(-90 10 ${py(0.5)})`} style={{ fontFamily: 'Inter,sans-serif' }}>TPR</text>
    </svg>
  );
}

// ─── Feature Importance ───────────────────────────────────────────────────────
function FeatureImportanceChart() {
  const feats = [
    ['days_since_expiry',   0.312], ['total_listen_time', 0.178], ['plan_days', 0.143],
    ['num_25_pct_played',   0.121], ['num_uniq_songs',    0.098], ['payment_plan_days', 0.067],
    ['registration_via',    0.045], ['city',              0.036],
  ];
  const max = feats[0][1];
  const ROW = 27, PL = 160, PR = 52, PT = 8;
  const W = 330, H = feats.length * ROW + PT * 2;
  const iW = W - PL - PR;

  return (
    <svg viewBox={`0 0 ${W} ${H}`} style={{ width: '100%' }}>
      {feats.map(([name, val], i) => {
        const y = PT + i * ROW;
        const bw = (val / max) * iW;
        return (
          <g key={name}>
            <text x={PL - 8} y={y + ROW / 2 + 4} textAnchor="end" fontSize="10.5" fill="#374151"
              style={{ fontFamily: 'monospace' }}>{name}</text>
            <rect x={PL} y={y + 5} width={bw} height={ROW - 10} fill={OG} fillOpacity="0.72" rx="2.5" />
            <text x={PL + bw + 5} y={y + ROW / 2 + 4} fontSize="10" fill="#9ca3af"
              style={{ fontFamily: 'Inter,sans-serif' }}>{(val * 100).toFixed(1)}%</text>
          </g>
        );
      })}
    </svg>
  );
}

Object.assign(window, { GaugeChart, DonutChart, HistogramBars, ROCCurve, FeatureImportanceChart, THRESHOLD });
