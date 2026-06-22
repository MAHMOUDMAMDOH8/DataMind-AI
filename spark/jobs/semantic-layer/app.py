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
    margin-left: auto;
  }

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
    margin: 8px 0;
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
  }

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
  }
  .results-table td {
    border: 1px solid var(--border2);
    padding: 5px 10px;
    font-family: 'JetBrains Mono', monospace;
    font-size: 12px;
  }
  .results-table tr:hover td {
    background: rgba(255,255,255,0.02);
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
  }
  .input-row input {
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
  }
  .input-row input:focus {
    border-color: var(--accent);
  }
  .input-row input::placeholder {
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
    color: var(--dim);
    font-size: 12px;
    padding: 4px 0;
    max-width: 800px;
    margin: 0 auto;
    width: 100%;
  }
  .loading .dots::after {
    content: '';
    animation: dots 1.5s infinite;
  }
  @keyframes dots {
    0% { content: ''; }
    25% { content: '.'; }
    50% { content: '..'; }
    75% { content: '...'; }
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
</style>
</head>
<body>
<div class="header">
  <div class="logo">
    <span class="cyan">data</span><span class="white">mind</span>
    <span class="yellow">AI</span>
  </div>
  <div class="sub">Semantic Layer</div>
</div>

<div class="chat" id="chat"></div>
<div class="loading" id="loading"><span class="dots">thinking</span></div>

<div class="input-area">
  <div class="input-row">
    <input id="input" type="text" placeholder="Ask a business question..." autofocus>
    <button id="send" onclick="ask()">Ask</button>
  </div>
  <div class="suggestions" id="suggestions"></div>
</div>

<script>
const chatEl = document.getElementById('chat');
const inputEl = document.getElementById('input');
const loadingEl = document.getElementById('loading');

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
  btn.onclick = () => { inputEl.value = s; ask(); };
  sugContainer.appendChild(btn);
});

function escape(str) {
  const d = document.createElement('div');
  d.textContent = str;
  return d.innerHTML;
}

function addMsg(msg) {
  if (msg.role === 'user') {
    const div = document.createElement('div');
    div.className = 'msg user';
    div.innerHTML = `<div class="bubble">${escape(msg.text)}</div>`;
    chatEl.appendChild(div);
  } else {
    const div = document.createElement('div');
    div.className = 'msg';
    let html = '<div class="bubble system">';

    if (msg.error) {
      html += `<div class="error-line">${escape(msg.error)}</div>`;
    }

    if (msg.sql) {
      html += `<div class="sql-block">${escape(msg.sql)}</div>`;
    }

    if (msg.rows && msg.rows.length > 0) {
      html += '<table class="results-table"><thead><tr>';
      msg.columns.forEach(c => { html += `<th>${escape(c)}</th>`; });
      html += '</tr></thead><tbody>';
      msg.rows.forEach(r => {
        html += '<tr>';
        r.forEach(c => { html += `<td>${escape(c != null ? String(c) : 'NULL')}</td>`; });
        html += '</tr>';
      });
      html += '</tbody></table>';
      html += `<div class="meta">${msg.row_count} row${msg.row_count !== 1 ? 's' : ''}`;
      if (msg.tokens) html += ` &middot; ${msg.tokens} tokens`;
      html += '</div>';
    }

    if (msg.explanation && !msg.sql) {
      html += `<div class="explain">${escape(msg.explanation)}</div>`;
    }

    html += '</div></div>';
    div.innerHTML = html;
    chatEl.appendChild(div);
  }
  chatEl.scrollTop = chatEl.scrollHeight;
}

async function ask() {
  const q = inputEl.value.trim();
  if (!q) return;
  inputEl.value = '';
  loadingEl.style.display = 'block';
  document.getElementById('send').disabled = true;

  addMsg({ role: 'user', text: q });

  try {
    const res = await fetch('/v1/chat', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ question: q }),
    });
    const data = await res.json();
    addMsg({ role: 'assistant', ...data });
  } catch (e) {
    addMsg({ role: 'assistant', error: 'Failed to connect to server' });
  }

  loadingEl.style.display = 'none';
  document.getElementById('send').disabled = false;
  inputEl.focus();
}

inputEl.addEventListener('keydown', e => {
  if (e.key === 'Enter' && !e.shiftKey) {
    e.preventDefault();
    ask();
  }
});
</script>
</body>
</html>"""


@app.get("/", response_class=HTMLResponse)
def web():
    return HTML


@app.get("/chat", response_class=HTMLResponse)
def web_chat():
    return HTML
