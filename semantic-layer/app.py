import os
import re
from pathlib import Path
from fastapi import FastAPI, HTTPException
from fastapi.responses import HTMLResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel
from openai import OpenAI
from dotenv import load_dotenv
from semantic import get_semantic_layer

load_dotenv(Path(__file__).resolve().parent.parent / ".env")

app = FastAPI(title="DataMind AI - Semantic Layer", version="1.0.0")

sl = get_semantic_layer()

OPENAI_API_KEY = os.getenv("OPENAI_API_KEY", "")
OPENAI_MODEL = os.getenv("OPENAI_MODEL", "deepseek-3.2")
OPENAI_BASE_URL = os.getenv("OPENAI_BASE_URL", "")

client_kwargs = {"api_key": OPENAI_API_KEY}
if OPENAI_BASE_URL:
    client_kwargs["base_url"] = OPENAI_BASE_URL
openai_client = OpenAI(**client_kwargs) if OPENAI_API_KEY else None


class SQLQueryRequest(BaseModel):
    sql: str


class SQLQueryResponse(BaseModel):
    columns: list[str] | None = None
    rows: list[list] | None = None
    row_count: int | None = None
    error: str | None = None
    sql: str | None = None


class ChatRequest(BaseModel):
    question: str


class ChatResponse(BaseModel):
    question: str
    sql: str | None = None
    columns: list[str] | None = None
    rows: list[list] | None = None
    row_count: int | None = None
    error: str | None = None
    explanation: str | None = None
    tokens: int | None = None


def extract_sql(text: str) -> str | None:
    match = re.search(r"```sql\n(.*?)```", text, re.DOTALL)
    if match:
        return match.group(1).strip()
    match = re.search(r"```\n(.*?)```", text, re.DOTALL)
    if match:
        return match.group(1).strip()
    lines = text.strip().split("\n")
    sql_lines = []
    in_sql = False
    for line in lines:
        s = line.strip()
        if s.upper().startswith("SELECT") and not in_sql:
            in_sql = True
            sql_lines.append(s)
        elif in_sql:
            if s and not s.startswith("```"):
                sql_lines.append(s)
            else:
                break
    if sql_lines:
        return " ".join(sql_lines)
    return None


@app.get("/health")
def health():
    return {"status": "ok", "metrics": len(sl.metrics),
            "dimensions": len(sl.dimensions)}


@app.get("/v1/context")
def context():
    return {"context": sl.get_context()}


@app.get("/v1/metrics")
def metrics():
    return {"metrics": sl.get_metrics_list()}


@app.get("/v1/dimensions")
def dimensions():
    return {"dimensions": sl.get_dimensions_list()}


@app.get("/v1/entities")
def entities():
    return {"entities": sl.get_entities_list()}


@app.get("/v1/relationships")
def relationships():
    return {"relationships": sl.get_relationships_list()}


@app.post("/v1/query", response_model=SQLQueryResponse)
def query(req: SQLQueryRequest):
    result = sl.execute_query(req.sql)
    if "error" in result:
        raise HTTPException(status_code=400, detail=result)
    return result


@app.post("/v1/chat", response_model=ChatResponse)
def chat(req: ChatRequest):
    if not openai_client:
        raise HTTPException(status_code=500, detail="OpenAI client not configured")

    context = sl.get_context()
    messages = [
        {"role": "system", "content": context},
        {"role": "user", "content": req.question},
    ]

    try:
        response = openai_client.chat.completions.create(
            model=OPENAI_MODEL,
            messages=messages,
            temperature=0.1,
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

    reply = response.choices[0].message.content
    tokens = response.usage.total_tokens if response.usage else None

    sql = extract_sql(reply)

    explanation = None
    if sql:
        explanation = reply[:reply.index(sql)].strip() if sql in reply else None
    else:
        explanation = reply

    if sql:
        result = sl.execute_query(sql)
        if "error" in result:
            return ChatResponse(
                question=req.question, sql=sql, error=result["error"],
                explanation=explanation, tokens=tokens,
            )
        return ChatResponse(
            question=req.question, sql=sql,
            columns=result["columns"], rows=result["rows"],
            row_count=result["row_count"],
            explanation=explanation, tokens=tokens,
        )

    return ChatResponse(
        question=req.question, explanation=reply, tokens=tokens,
    )


HTML = r"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>DataMind AI — Semantic Layer</title>
<style>
  @import url('https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700&family=JetBrains+Mono:wght@400;500&display=swap');

  :root {
    --bg: #0d0d0d;
    --surface: #141414;
    --surface2: #1a1a1a;
    --border: #222;
    --border2: #2a2a2a;
    --text: #e0e0e0;
    --dim: #666;
    --accent: #00d4aa;
    --accent-dim: rgba(0,212,170,0.1);
    --green: #00d4aa;
    --blue: #60a5fa;
    --red: #f87171;
    --yellow: #fbbf24;
  }

  * { margin: 0; padding: 0; box-sizing: border-box; }
  body {
    background: var(--bg);
    color: var(--text);
    font-family: 'Inter', -apple-system, sans-serif;
    height: 100vh;
    display: flex;
    flex-direction: column;
    font-size: 14px;
    line-height: 1.5;
  }

  .header {
    border-bottom: 1px solid var(--border);
    padding: 16px 24px;
    display: flex;
    align-items: center;
    gap: 12px;
    flex-shrink: 0;
  }
  .header .logo {
    font-family: 'JetBrains Mono', monospace;
    font-weight: 700;
    font-size: 18px;
    letter-spacing: -0.5px;
  }
  .header .logo .cyan { color: var(--accent); }
  .header .logo .white { color: var(--text); }
  .header .logo .yellow { color: var(--yellow); }
  .header .sub {
    color: var(--dim);
    font-size: 12px;
  }
  .header .right {
    margin-left: auto;
    display: flex;
    align-items: center;
    gap: 16px;
  }
  .header .status {
    display: flex;
    align-items: center;
    gap: 6px;
    font-size: 11px;
    color: var(--dim);
    font-family: 'JetBrains Mono', monospace;
  }
  .header .status .dot {
    width: 7px;
    height: 7px;
    border-radius: 50%;
    background: var(--dim);
    transition: background 0.2s, box-shadow 0.2s;
  }
  .header .status .dot.ok { background: var(--accent); box-shadow: 0 0 6px var(--accent); }
  .header .status .dot.err { background: var(--red); box-shadow: 0 0 6px var(--red); }
  .header .clear-btn {
    background: transparent;
    border: 1px solid var(--border2);
    color: var(--dim);
    border-radius: 6px;
    padding: 5px 12px;
    font-size: 11px;
    cursor: pointer;
    transition: all 0.2s;
  }
  .header .clear-btn:hover { border-color: var(--red); color: var(--red); }

  .chat {
    flex: 1;
    overflow-y: auto;
    padding: 16px 24px;
    display: flex;
    flex-direction: column;
    gap: 12px;
  }
  .chat:empty::after {
    content: 'Ask a business question about your telecom data.';
    color: var(--dim);
    font-style: italic;
    text-align: center;
    padding: 40px 0;
  }

  .msg {
    max-width: 800px;
    width: 100%;
    margin: 0 auto;
  }
  .msg.user {
    text-align: right;
  }
  .msg.user .bubble {
    background: var(--accent-dim);
    border: 1px solid rgba(0,212,170,0.2);
    border-radius: 12px 12px 4px 12px;
    padding: 10px 14px;
    display: inline-block;
    font-size: 13px;
    color: var(--text);
    text-align: left;
    max-width: 70%;
    white-space: pre-wrap;
  }
  .msg .bubble.system {
    background: var(--surface);
    border: 1px solid var(--border);
    border-radius: 12px 12px 12px 4px;
    padding: 14px;
  }
  .msg .bubble.system .sql-block {
    background: var(--surface2);
    border: 1px solid var(--border2);
    border-radius: 8px;
    padding: 10px 12px;
    margin: 8px 0 4px;
    font-family: 'JetBrains Mono', monospace;
    font-size: 12px;
    color: var(--green);
    overflow-x: auto;
    white-space: pre;
  }
  .msg .bubble.system .explain {
    font-size: 13px;
    color: var(--dim);
    line-height: 1.6;
    white-space: pre-wrap;
  }
  .msg .bubble.system .explain code {
    background: var(--surface2);
    border: 1px solid var(--border2);
    border-radius: 4px;
    padding: 1px 5px;
    font-family: 'JetBrains Mono', monospace;
    font-size: 12px;
    color: var(--blue);
  }
  .msg .bubble.system .explain strong { color: var(--text); }

  .block-actions {
    display: flex;
    gap: 6px;
    margin: 0 0 10px;
  }
  .block-actions button {
    background: transparent;
    border: 1px solid var(--border2);
    color: var(--dim);
    border-radius: 6px;
    padding: 3px 10px;
    font-size: 10px;
    cursor: pointer;
    font-family: 'JetBrains Mono', monospace;
    transition: all 0.2s;
  }
  .block-actions button:hover { border-color: var(--accent); color: var(--accent); }

  .results-table {
    border-collapse: collapse;
    margin: 8px 0;
    width: 100%;
    font-size: 12px;
  }
  .results-table th {
    background: var(--surface2);
    border: 1px solid var(--border2);
    padding: 6px 10px;
    text-align: left;
    font-weight: 600;
    color: var(--accent);
    font-family: 'JetBrains Mono', monospace;
    font-size: 11px;
    text-transform: uppercase;
    letter-spacing: 0.5px;
    position: sticky;
    top: 0;
  }
  .results-table th.num, .results-table td.num {
    text-align: right;
    font-variant-numeric: tabular-nums;
  }
  .results-table td {
    border: 1px solid var(--border2);
    padding: 5px 10px;
    font-family: 'JetBrains Mono', monospace;
    font-size: 12px;
  }
  .results-table td.null-cell {
    color: var(--dim);
    font-style: italic;
  }
  .results-table tr:hover td {
    background: rgba(255,255,255,0.02);
  }
  .table-wrap {
    max-height: 360px;
    overflow: auto;
    border-radius: 8px;
  }
  .meta {
    font-size: 11px;
    color: var(--dim);
    margin-top: 6px;
  }
  .error-line {
    color: var(--red);
    font-size: 12px;
    padding: 6px 0;
  }

  .chart-wrap {
    margin: 0 0 10px;
    background: var(--surface2);
    border: 1px solid var(--border2);
    border-radius: 8px;
    padding: 10px 14px;
  }
  .chart-wrap svg { width: 100%; height: auto; display: block; }
  .chart-wrap.pie-wrap {
    display: flex;
    align-items: center;
    gap: 24px;
    flex-wrap: wrap;
  }
  .chart-wrap.pie-wrap svg { width: 160px; height: 160px; flex-shrink: 0; }
  .pie-legend {
    display: flex;
    flex-direction: column;
    gap: 6px;
    font-size: 11px;
    color: var(--text);
    font-family: 'JetBrains Mono', monospace;
  }
  .pie-legend-item { display: flex; align-items: center; gap: 6px; }
  .pie-legend .swatch { width: 10px; height: 10px; border-radius: 2px; display: inline-block; flex-shrink: 0; }
  .pie-legend .pie-pct { color: var(--dim); margin-left: 2px; }

  .chart-toggle {
    display: flex;
    gap: 6px;
    margin: 8px 0;
  }
  .chart-toggle button {
    background: transparent;
    border: 1px solid var(--border2);
    color: var(--dim);
    border-radius: 6px;
    padding: 3px 10px;
    font-size: 10px;
    cursor: pointer;
    font-family: 'JetBrains Mono', monospace;
    transition: all 0.2s;
  }
  .chart-toggle button:hover { border-color: var(--accent); color: var(--accent); }
  .chart-toggle button.active { border-color: var(--accent); color: var(--accent); background: var(--accent-dim); }

  .input-area {
    border-top: 1px solid var(--border);
    padding: 12px 24px 16px;
    flex-shrink: 0;
  }
  .input-row {
    max-width: 800px;
    margin: 0 auto;
    display: flex;
    gap: 8px;
    align-items: flex-end;
  }
  .input-row textarea {
    flex: 1;
    background: var(--surface);
    border: 1px solid var(--border);
    border-radius: 8px;
    padding: 10px 14px;
    color: var(--text);
    font-family: 'Inter', sans-serif;
    font-size: 13px;
    outline: none;
    transition: border-color 0.2s;
    resize: none;
    max-height: 140px;
    line-height: 1.4;
  }
  .input-row textarea:focus {
    border-color: var(--accent);
  }
  .input-row textarea::placeholder {
    color: var(--dim);
  }
  .input-row button {
    background: var(--accent);
    color: #000;
    border: none;
    border-radius: 8px;
    padding: 10px 20px;
    font-weight: 600;
    font-size: 13px;
    cursor: pointer;
    transition: opacity 0.2s;
    white-space: nowrap;
  }
  .input-row button:hover { opacity: 0.85; }
  .input-row button:disabled {
    opacity: 0.4;
    cursor: not-allowed;
  }

  .loading {
    display: none;
    align-items: center;
    gap: 8px;
    color: var(--dim);
    font-size: 12px;
    padding: 4px 0;
    max-width: 800px;
    margin: 0 auto;
    width: 100%;
  }
  .loading .typing {
    display: flex;
    gap: 3px;
  }
  .loading .typing span {
    width: 5px;
    height: 5px;
    border-radius: 50%;
    background: var(--accent);
    animation: bounce 1.2s infinite ease-in-out;
  }
  .loading .typing span:nth-child(2) { animation-delay: 0.15s; }
  .loading .typing span:nth-child(3) { animation-delay: 0.3s; }
  @keyframes bounce {
    0%, 60%, 100% { transform: translateY(0); opacity: 0.4; }
    30% { transform: translateY(-4px); opacity: 1; }
  }

  .suggestions {
    display: flex;
    gap: 8px;
    flex-wrap: wrap;
    max-width: 800px;
    margin: 8px auto 0;
  }
  .suggestions button {
    background: transparent;
    border: 1px solid var(--border2);
    border-radius: 20px;
    padding: 4px 12px;
    color: var(--dim);
    font-size: 11px;
    cursor: pointer;
    transition: all 0.2s;
  }
  .suggestions button:hover {
    border-color: var(--accent);
    color: var(--accent);
  }

  .scroll-btn {
    position: fixed;
    bottom: 96px;
    right: 24px;
    width: 34px;
    height: 34px;
    border-radius: 50%;
    background: var(--surface);
    border: 1px solid var(--border2);
    color: var(--accent);
    display: none;
    align-items: center;
    justify-content: center;
    cursor: pointer;
    font-size: 16px;
    box-shadow: 0 2px 10px rgba(0,0,0,0.5);
    transition: opacity 0.2s;
  }
  .scroll-btn.show { display: flex; }
  .scroll-btn:hover { border-color: var(--accent); }
</style>
</head>
<body>
<div class="header">
  <div class="logo">
    <span class="cyan">data</span><span class="white">mind</span>
    <span class="yellow">AI</span>
  </div>
  <div class="sub">Semantic Layer</div>
  <div class="right">
    <div class="status"><span class="dot" id="status-dot"></span><span id="status-text">connecting…</span></div>
    <button class="clear-btn" id="clear-btn">Clear</button>
  </div>
</div>

<div class="chat" id="chat"></div>
<button class="scroll-btn" id="scroll-btn" title="Scroll to latest">&darr;</button>
<div class="loading" id="loading">
  <span class="typing"><span></span><span></span><span></span></span>
  <span>thinking</span>
</div>

<div class="input-area">
  <div class="input-row">
    <textarea id="input" rows="1" placeholder="Ask a business question... (Shift+Enter for newline)" autofocus></textarea>
    <button id="send" onclick="ask()">Ask</button>
  </div>
  <div class="suggestions" id="suggestions"></div>
</div>

<script>
const chatEl = document.getElementById('chat');
const inputEl = document.getElementById('input');
const loadingEl = document.getElementById('loading');
const scrollBtn = document.getElementById('scroll-btn');
const statusDot = document.getElementById('status-dot');
const statusText = document.getElementById('status-text');
const STORAGE_KEY = 'datamind_chat_history';

let messages = [];

const suggestions = [
  'total revenue last month',
  'top 5 customers by revenue',
  'network health score by region',
  'how many active customers',
  'average revenue per user',
  'payment success rate',
];

const sugContainer = document.getElementById('suggestions');
suggestions.forEach(s => {
  const btn = document.createElement('button');
  btn.textContent = s;
  btn.onclick = () => ask(s);
  sugContainer.appendChild(btn);
});

function escape(str) {
  const d = document.createElement('div');
  d.textContent = str;
  return d.innerHTML;
}

function numberFmt(v) {
  if (v === null || v === undefined || isNaN(v)) return String(v);
  return Number.isInteger(v)
    ? v.toLocaleString()
    : v.toLocaleString(undefined, { minimumFractionDigits: 2, maximumFractionDigits: 2 });
}

function isNumericColumn(colIdx, rows) {
  return rows.every(r => r[colIdx] === null || (r[colIdx] !== '' && !isNaN(parseFloat(r[colIdx])) && isFinite(r[colIdx])));
}

function formatExplanation(text) {
  let html = escape(text);
  html = html.replace(/`([^`]+)`/g, '<code>$1</code>');
  html = html.replace(/\*\*([^*]+)\*\*/g, '<strong>$1</strong>');
  return html;
}

function buildTable(msg) {
  const numericCols = msg.columns.map((_, i) => isNumericColumn(i, msg.rows));
  let html = '<div class="table-wrap"><table class="results-table"><thead><tr>';
  msg.columns.forEach((c, i) => { html += `<th class="${numericCols[i] ? 'num' : ''}">${escape(c)}</th>`; });
  html += '</tr></thead><tbody>';
  msg.rows.forEach(r => {
    html += '<tr>';
    r.forEach((c, i) => {
      const isNum = numericCols[i];
      if (c === null) {
        html += `<td class="null-cell${isNum ? ' num' : ''}">NULL</td>`;
      } else {
        const display = isNum ? numberFmt(parseFloat(c)) : String(c);
        html += `<td class="${isNum ? 'num' : ''}">${escape(display)}</td>`;
      }
    });
    html += '</tr>';
  });
  html += '</tbody></table></div>';
  return html;
}

const PIE_COLORS = ['#00d4aa', '#60a5fa', '#fbbf24', '#f87171', '#c084fc', '#34d399', '#f472b6', '#94a3b8'];

function looksLikeTimeSeries(vals) {
  const dateRe = /^\d{4}-\d{2}(-\d{2})?/;
  return vals.length > 1 && vals.every(v => typeof v === 'string' && dateRe.test(v));
}

function getAvailableChartTypes(columns, rows) {
  if (columns.length !== 2 || rows.length < 2 || !isNumericColumn(1, rows)) return [];
  const types = ['bar', 'line'];
  if (rows.length <= 8 && rows.every(r => r[1] === null || parseFloat(r[1]) >= 0)) types.push('pie');
  return types;
}

function detectDefaultChartType(columns, rows) {
  const types = getAvailableChartTypes(columns, rows);
  if (!types.length) return null;
  if (looksLikeTimeSeries(rows.map(r => r[0]))) return 'line';
  return 'bar';
}

function buildChartControls(idx, types, active) {
  let html = '<div class="chart-toggle">';
  types.forEach(t => {
    html += `<button class="${t === active ? 'active' : ''}" data-chart="${idx}" data-type="${t}">${t[0].toUpperCase()}${t.slice(1)}</button>`;
  });
  html += '</div>';
  return html;
}

function renderChart(type, columns, rows) {
  if (type === 'line') return lineChart(columns, rows);
  if (type === 'pie') return pieChart(columns, rows);
  return barChart(columns, rows);
}

function barChart(columns, rows) {
  const chartRows = rows.slice(0, 10).filter(r => r[1] !== null);
  if (!chartRows.length) return '';
  const vals = chartRows.map(r => parseFloat(r[1]));
  const max = Math.max(...vals, 0.0001);
  const w = 640, barH = 20, gap = 10, padLeft = 130, padRight = 60;
  const h = chartRows.length * (barH + gap) + gap;
  let bars = '';
  chartRows.forEach((r, i) => {
    const v = parseFloat(r[1]);
    const bw = Math.max((v / max) * (w - padLeft - padRight), 2);
    const y = gap + i * (barH + gap);
    const label = String(r[0]).length > 16 ? String(r[0]).slice(0, 15) + '…' : String(r[0]);
    bars += `<text x="${padLeft - 8}" y="${y + barH / 2 + 4}" text-anchor="end" style="font:10px 'JetBrains Mono',monospace;fill:var(--dim)">${escape(label)}</text>`;
    bars += `<rect x="${padLeft}" y="${y}" width="${bw}" height="${barH}" rx="3" style="fill:var(--accent);opacity:0.85" />`;
    bars += `<text x="${padLeft + bw + 6}" y="${y + barH / 2 + 4}" style="font:10px 'JetBrains Mono',monospace;fill:var(--text)">${numberFmt(v)}</text>`;
  });
  return `<div class="chart-wrap"><svg viewBox="0 0 ${w} ${h}" xmlns="http://www.w3.org/2000/svg">${bars}</svg></div>`;
}

function lineChart(columns, rows) {
  const pts = rows.filter(r => r[1] !== null).map(r => ({ x: String(r[0]), y: parseFloat(r[1]) }));
  if (pts.length < 2) return '';
  const w = 640, h = 220, padL = 56, padR = 20, padT = 16, padB = 30;
  const ys = pts.map(p => p.y);
  const minY = Math.min(0, ...ys);
  const maxY = Math.max(...ys, minY + 0.0001);
  const stepX = pts.length > 1 ? (w - padL - padR) / (pts.length - 1) : 0;
  const scaleY = v => h - padB - ((v - minY) / (maxY - minY)) * (h - padT - padB);
  const everyN = Math.max(1, Math.ceil(pts.length / 10));
  let path = '';
  let dots = '';
  let labels = '';
  pts.forEach((p, i) => {
    const x = padL + i * stepX;
    const y = scaleY(p.y);
    path += (i === 0 ? 'M' : 'L') + x.toFixed(1) + ',' + y.toFixed(1) + ' ';
    dots += `<circle cx="${x.toFixed(1)}" cy="${y.toFixed(1)}" r="3" style="fill:var(--accent)"><title>${escape(p.x)}: ${numberFmt(p.y)}</title></circle>`;
    if (i % everyN === 0 || i === pts.length - 1) {
      const label = String(p.x).length > 10 ? String(p.x).slice(-10) : String(p.x);
      labels += `<text x="${x.toFixed(1)}" y="${h - padB + 16}" text-anchor="middle" style="font:9px 'JetBrains Mono',monospace;fill:var(--dim)">${escape(label)}</text>`;
    }
  });
  const zeroY = scaleY(0);
  const gridline = minY < 0
    ? `<line x1="${padL}" y1="${zeroY.toFixed(1)}" x2="${w - padR}" y2="${zeroY.toFixed(1)}" style="stroke:var(--border2);stroke-width:1" />`
    : '';
  return `<div class="chart-wrap"><svg viewBox="0 0 ${w} ${h}" xmlns="http://www.w3.org/2000/svg">${gridline}<path d="${path.trim()}" fill="none" style="stroke:var(--accent);stroke-width:2" />${dots}${labels}</svg></div>`;
}

function pieChart(columns, rows) {
  const data = rows.filter(r => r[1] !== null).map(r => ({ label: String(r[0]), value: Math.max(parseFloat(r[1]), 0) })).slice(0, 8);
  const total = data.reduce((s, d) => s + d.value, 0);
  if (!total) return '';
  const cx = 80, cy = 80, r = 76;
  let angle = -Math.PI / 2;
  let slices = '';
  let legend = '';
  data.forEach((d, i) => {
    const frac = d.value / total;
    const nextAngle = angle + frac * Math.PI * 2;
    const x1 = cx + r * Math.cos(angle), y1 = cy + r * Math.sin(angle);
    const x2 = cx + r * Math.cos(nextAngle), y2 = cy + r * Math.sin(nextAngle);
    const large = frac > 0.5 ? 1 : 0;
    const color = PIE_COLORS[i % PIE_COLORS.length];
    slices += `<path d="M${cx},${cy} L${x1.toFixed(2)},${y1.toFixed(2)} A${r},${r} 0 ${large} 1 ${x2.toFixed(2)},${y2.toFixed(2)} Z" style="fill:${color};opacity:0.88"><title>${escape(d.label)}: ${numberFmt(d.value)} (${(frac * 100).toFixed(1)}%)</title></path>`;
    legend += `<div class="pie-legend-item"><span class="swatch" style="background:${color}"></span>${escape(d.label)}<span class="pie-pct">${(frac * 100).toFixed(1)}%</span></div>`;
    angle = nextAngle;
  });
  return `<div class="chart-wrap pie-wrap"><svg viewBox="0 0 160 160" xmlns="http://www.w3.org/2000/svg">${slices}</svg><div class="pie-legend">${legend}</div></div>`;
}

function exportCSV(msg) {
  if (!msg.columns || !msg.rows) return;
  const esc = v => `"${String(v ?? '').replace(/"/g, '""')}"`;
  const lines = [msg.columns.map(esc).join(',')];
  msg.rows.forEach(r => lines.push(r.map(esc).join(',')));
  const blob = new Blob([lines.join('\n')], { type: 'text/csv' });
  const a = document.createElement('a');
  a.href = URL.createObjectURL(blob);
  a.download = 'datamind-export.csv';
  document.body.appendChild(a);
  a.click();
  a.remove();
  URL.revokeObjectURL(a.href);
}

function saveHistory() {
  try { localStorage.setItem(STORAGE_KEY, JSON.stringify(messages)); } catch (e) {}
}

function loadHistory() {
  try {
    const raw = localStorage.getItem(STORAGE_KEY);
    if (raw) messages = JSON.parse(raw);
  } catch (e) {
    messages = [];
  }
}

function render() {
  chatEl.innerHTML = '';
  messages.forEach((msg, idx) => {
    const div = document.createElement('div');
    if (msg.role === 'user') {
      div.className = 'msg user';
      div.innerHTML = `<div class="bubble">${escape(msg.text)}</div>`;
    } else {
      div.className = 'msg';
      let html = '<div class="bubble system">';

      if (msg.error) {
        html += `<div class="error-line">${escape(msg.error)}</div>`;
      }

      if (msg.sql) {
        html += `<div class="sql-block">${escape(msg.sql)}</div>`;
        html += '<div class="block-actions">';
        html += `<button data-copy="${idx}">Copy SQL</button>`;
        if (msg.rows && msg.rows.length) html += `<button data-export="${idx}">Export CSV</button>`;
        html += '</div>';
      }

      if (msg.rows && msg.rows.length > 0) {
        html += buildTable(msg);
        const chartTypes = getAvailableChartTypes(msg.columns, msg.rows);
        if (chartTypes.length) {
          if (!msg.chartType || !chartTypes.includes(msg.chartType)) {
            msg.chartType = detectDefaultChartType(msg.columns, msg.rows) || chartTypes[0];
          }
          html += buildChartControls(idx, chartTypes, msg.chartType);
          html += renderChart(msg.chartType, msg.columns, msg.rows);
        }
        html += `<div class="meta">${msg.row_count} row${msg.row_count !== 1 ? 's' : ''}`;
        if (msg.tokens) html += ` &middot; ${msg.tokens} tokens`;
        html += '</div>';
      }

      if (msg.explanation && !msg.sql) {
        html += `<div class="explain">${formatExplanation(msg.explanation)}</div>`;
      }

      html += '</div>';
      div.innerHTML = html;
    }
    chatEl.appendChild(div);
  });
  chatEl.scrollTop = chatEl.scrollHeight;
}

chatEl.addEventListener('click', e => {
  const copyBtn = e.target.closest('[data-copy]');
  if (copyBtn) {
    const msg = messages[copyBtn.dataset.copy];
    navigator.clipboard.writeText(msg.sql || '').then(() => {
      const original = copyBtn.textContent;
      copyBtn.textContent = 'Copied';
      setTimeout(() => { copyBtn.textContent = original; }, 1200);
    });
    return;
  }
  const exportBtn = e.target.closest('[data-export]');
  if (exportBtn) exportCSV(messages[exportBtn.dataset.export]);
  const chartBtn = e.target.closest('[data-chart]');
  if (chartBtn) {
    messages[chartBtn.dataset.chart].chartType = chartBtn.dataset.type;
    saveHistory();
    render();
  }
});

chatEl.addEventListener('scroll', () => {
  const nearBottom = chatEl.scrollHeight - chatEl.scrollTop - chatEl.clientHeight < 80;
  scrollBtn.classList.toggle('show', !nearBottom && chatEl.scrollHeight > chatEl.clientHeight + 40);
});
scrollBtn.onclick = () => chatEl.scrollTo({ top: chatEl.scrollHeight, behavior: 'smooth' });

document.getElementById('clear-btn').onclick = () => {
  messages = [];
  saveHistory();
  render();
};

async function pollHealth() {
  try {
    const res = await fetch('/health');
    if (!res.ok) throw new Error('bad status');
    const data = await res.json();
    statusDot.className = 'dot ok';
    statusText.textContent = `${data.metrics} metrics online`;
  } catch (e) {
    statusDot.className = 'dot err';
    statusText.textContent = 'offline';
  }
}

async function ask(preset) {
  const q = (preset !== undefined ? preset : inputEl.value).trim();
  if (!q) return;
  inputEl.value = '';
  inputEl.style.height = 'auto';
  loadingEl.style.display = 'flex';
  document.getElementById('send').disabled = true;

  messages.push({ role: 'user', text: q });
  saveHistory();
  render();

  try {
    const res = await fetch('/v1/chat', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ question: q }),
    });
    const data = await res.json();
    messages.push({ role: 'assistant', ...data });
  } catch (e) {
    messages.push({ role: 'assistant', error: 'Failed to connect to server' });
  }

  saveHistory();
  render();
  loadingEl.style.display = 'none';
  document.getElementById('send').disabled = false;
  inputEl.focus();
}

inputEl.addEventListener('input', () => {
  inputEl.style.height = 'auto';
  inputEl.style.height = Math.min(inputEl.scrollHeight, 140) + 'px';
});

inputEl.addEventListener('keydown', e => {
  if (e.key === 'Enter' && !e.shiftKey) {
    e.preventDefault();
    ask();
  }
});

loadHistory();
render();
pollHealth();
setInterval(pollHealth, 15000);
</script>
</body>
</html>"""


@app.get("/", response_class=HTMLResponse)
def web():
    return HTML


@app.get("/chat", response_class=HTMLResponse)
def web_chat():
    return HTML
