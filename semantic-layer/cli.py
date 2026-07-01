import os
import sys
import re
import time
from pathlib import Path
from openai import OpenAI
from dotenv import load_dotenv
from rich.console import Console
from rich.table import Table
from rich.panel import Panel
from rich.syntax import Syntax
from rich.markdown import Markdown
from rich.text import Text
from rich.prompt import Prompt

from rich import box
from rich.style import Style
from rich.align import Align
from rich.rule import Rule
from prompt_toolkit import PromptSession
from prompt_toolkit.history import FileHistory
from prompt_toolkit.formatted_text import HTML
from prompt_toolkit.completion import WordCompleter
import csv
import plotext as plt
from semantic import SemanticLayer

load_dotenv(Path(__file__).resolve().parent.parent / ".env")

OPENAI_API_KEY = os.getenv("OPENAI_API_KEY", "")
OPENAI_MODEL = os.getenv("OPENAI_MODEL", "llama-3.3-70b-versatile")
OPENAI_BASE_URL = os.getenv("OPENAI_BASE_URL", None)
HISTORY_FILE = Path.home() / ".datamind_chat_history"

console = Console()

LOGO = r"""
[bold cyan]    ____  _  ____ ___  __  __ ___ _   _ ____
   |  _ \/ |/ ___/ _ \|  \/  |_ _| \ | |  _ \
   | | | | | |  | | | | |\/| || ||  \| | | | |
   | |_| | | |__| |_| | |  | || || |\  | |_| |
   |____/|_|\____\___/|_|  |_|___|_| \_|____/
[/bold cyan]
[bold yellow]   __  __ ___ _   _ ____  __  __    _    ___
  |  \/  |_ _| \ | |  _ \|  \/  |  / \  |_ _|
  | |\/| || ||  \| | | | | |\/| | / _ \  | |
  | |  | || || |\  | |_| | |  | |/ ___ \ | |
  |_|  |_|___|_| \_|____/|_|  |_/_/   \_\___|
[/bold yellow]
"""

EXAMPLES = [
    "total revenue last month",
    "top 5 customers by revenue",
    "network health score by region",
    "active customers count",
    "avg recharge amount",
    "payment success rate",
    "fraud events this week",
    "daily revenue trend",
]


VERSION = "1.0.0"


def print_logo():
    console.clear()
    console.print()
    subtitle = Text.assemble(
        ("AI-Powered Semantic Query Engine", "italic bright_black"),
        "   ",
        (f"v{VERSION}", "dim"),
        justify="center",
    )
    body = Text.from_markup(LOGO.strip())
    console.print(Panel(
        Align.center(Text.assemble(body, "\n", subtitle)),
        border_style="cyan", box=box.HEAVY, style="on grey11", padding=(1, 2),
    ))
    console.print()


def get_system_prompt(sl):
    return sl.get_context()


def extract_sql(text):
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


def is_number(v):
    if v is None:
        return False
    if isinstance(v, bool):
        return False
    if isinstance(v, (int, float)):
        return True
    try:
        float(str(v))
        return True
    except ValueError:
        return False


def format_cell(v):
    if v is None:
        return "[dim italic]NULL[/dim italic]"
    if isinstance(v, bool):
        return str(v)
    if isinstance(v, int):
        return f"{v:,}"
    if isinstance(v, float):
        return f"{v:,.2f}" if not v.is_integer() else f"{v:,.0f}"
    return str(v)


def print_table(columns, rows, max_rows=20):
    table = Table(
        box=box.ROUNDED,
        border_style="bright_blue",
        header_style="bold bright_cyan",
        show_edge=True,
        padding=(0, 2),
        row_styles=["none", "on grey11"],
    )

    display_rows = rows[:max_rows]
    numeric_cols = [
        all(is_number(r[i]) for r in display_rows if r[i] is not None)
        for i in range(len(columns))
    ] if display_rows else [False] * len(columns)

    for col, is_num in zip(columns, numeric_cols):
        table.add_column(col, justify="right" if is_num else "left", style="bright_green" if is_num else "bright_white")

    for row in display_rows:
        table.add_row(*[format_cell(c) for c in row])

    if rows:
        console.print(table)
        if len(rows) > max_rows:
            console.print(f"  [dim]Showing {max_rows} of {len(rows)} rows - use /export to see all[/dim]")
    else:
        console.print(Panel("[dim]No results[/dim]", border_style="dim", box=box.SIMPLE))

def plot_results(columns, rows):
    if len(columns) != 2 or not rows:
        return
    try:
        y_vals = []
        x_vals = []
        for r in rows:
            if r[1] is None: continue
            try:
                y_vals.append(float(r[1]))
                x_vals.append(str(r[0]))
            except ValueError:
                return
        if not y_vals: return
        
        plt.clf()
        plt.theme("clear")
        plt.plotsize(80, 15)
        
        plt.bar(x_vals, y_vals, color="cyan")
        plt.title(f"{columns[1]} by {columns[0]}")
        
        chart_str = plt.build()
        console.print()
        console.print(Panel(Text.from_ansi(chart_str), border_style="cyan", box=box.ROUNDED, padding=(1, 2), title="[bold]Chart[/bold]"))
    except Exception:
        pass


HELP_COMMANDS = [
    ("/help", "Show this help"),
    ("/exit, /quit", "Exit the chat"),
    ("/clear", "Clear screen and reset conversation"),
    ("/context", "Show full semantic context"),
    ("/metrics", "List all metrics"),
    ("/examples", "Show example questions"),
    ("/explain <q>", "Explain SQL without running it"),
    (r"/export \[file]", "Export last query to CSV"),
    ("/multiline", "Toggle multiline input mode"),
    ("/stats", "Show session statistics"),
    ("/model", "Show model info"),
]


def print_help():
    table = Table(box=None, show_header=False, padding=(0, 2), style="on grey11")
    table.add_column("", style="bold green", no_wrap=True)
    table.add_column("", style="dim")
    for cmd, desc in HELP_COMMANDS:
        table.add_row(cmd, desc)
    console.print(Panel(
        table,
        title="[bold]Commands[/bold]", border_style="bright_blue", box=box.ROUNDED, style="on grey11", padding=(1, 2)
    ))


def print_examples(sl):
    table = Table(box=None, show_header=False, padding=(0, 1), style="on grey11")
    table.add_column("", style="bright_cyan bold", width=4)
    table.add_column("", style="bright_white")
    for ex in EXAMPLES:
        table.add_row(">", ex)
    console.print(Panel(table, title="[bold]Try asking[/bold]", border_style="bright_magenta", box=box.ROUNDED, style="on grey11", padding=(1, 2)))


def print_metrics(sl):
    table = Table(box=box.SIMPLE_HEAVY, show_header=True, header_style="bold bright_cyan", border_style="bright_blue", padding=(0, 1), style="on grey11")
    table.add_column("Name", style="bright_white")
    table.add_column("Label", style="white")
    table.add_column("Table", style="dim")
    for m in sl.get_metrics_list():
        table.add_row(m["name"], m["label"], m.get("table", ""))
    console.print(Panel(table, title="[bold]Available Metrics[/bold]", border_style="bright_blue", box=box.ROUNDED, style="on grey11", padding=(1, 2)))


def chat():
    if not OPENAI_API_KEY:
        console.print("\n[red]![/red] OPENAI_API_KEY is not set\n")
        console.print("  Get a free key at [underline]https://console.groq.com[/underline]")
        console.print('  [dim]$env:OPENAI_API_KEY = "gsk_..."[/dim]')
        console.print('  [dim]$env:OPENAI_BASE_URL = "https://api.groq.com/openai/v1"[/dim]\n')
        sys.exit(1)

    print_logo()

    sl = SemanticLayer()
    console.print(Panel(
        Text.assemble(
            ("metrics", "bright_cyan"), "  ", (f"{len(sl.metrics)}", "bold bright_white"), "  |  ",
            ("dimensions", "bright_cyan"), "  ", (f"{len(sl.dimensions)}", "bold bright_white"), "  |  ",
            ("entities", "bright_cyan"), "  ", (f"{len(sl.entities)}", "bold bright_white"), "  |  ",
            ("relationships", "bright_cyan"), "  ", (f"{len(sl.relationships)}", "bold bright_white"),
            justify="center"
        ),
        box=box.ROUNDED, border_style="bright_cyan", padding=(1, 2), style="on grey15", title="[bold]Datamind Setup[/bold]"
    ))

    print_examples(sl)
    console.print(Rule("[bold bright_green]Ready[/bold bright_green]", style="dim green"))
    console.print()

    system_prompt = get_system_prompt(sl)

    client_kwargs = {"api_key": OPENAI_API_KEY}
    if OPENAI_BASE_URL:
        client_kwargs["base_url"] = OPENAI_BASE_URL
    client = OpenAI(**client_kwargs)

    messages = [{"role": "system", "content": system_prompt}]

    completion_words = []
    for m in sl.get_metrics_list():
        completion_words.extend([m["name"], m["label"].lower()])
    for d in sl.get_dimensions_list():
        completion_words.extend([d["name"], d.get("label", "").lower()])
    for e in sl.get_entities_list():
        completion_words.extend([e["name"], e.get("label", "").lower()])
    completion_words = list(set([w for w in completion_words if w]))
    
    completer = WordCompleter(completion_words, ignore_case=True)
    
    def get_bottom_toolbar():
        mode = "ON" if is_multiline else "OFF"
        return HTML(
            f' <b>Ctrl+C</b> exit  │  <b>/help</b>  │  '
            f'Multiline: <b>{mode}</b>  │  Queries: <b>{stats["queries"]}</b>  │  '
            f'<style bg="ansiblue"> {OPENAI_MODEL} </style> '
        )

    is_multiline = False
    stats = {"queries": 0, "errors": 0, "tokens": 0, "llm_time": 0.0, "sql_time": 0.0}
    session_start = time.time()

    session = PromptSession(
        history=FileHistory(str(HISTORY_FILE)),
        completer=completer,
        bottom_toolbar=get_bottom_toolbar,
    )

    last_result = None

    while True:
        try:
            if is_multiline:
                console.print("  [dim](Multiline mode: press Esc then Enter to submit)[/dim]")
            question = session.prompt(HTML('<ansicyan>&gt;</ansicyan> '), multiline=is_multiline)
        except (EOFError, KeyboardInterrupt):
            break

        question = question.strip()
        if not question:
            continue

        cmd = question.lower()

        if cmd in ("/exit", "/quit"):
            break
        if cmd == "/clear":
            messages = [messages[0]]
            print_logo()
            continue
        if cmd == "/help":
            print_help()
            continue
        if cmd == "/examples":
            print_examples(sl)
            continue
        if cmd == "/context":
            console.print(Panel(system_prompt, title="Context", border_style="dim", box=box.SIMPLE))
            continue
        if cmd == "/model":
            console.print(Panel(
                Text.assemble(
                    ("Provider", "bright_cyan"), f"  {OPENAI_BASE_URL or 'https://api.openai.com/v1'}\n",
                    ("Model", "bright_cyan"), "    ", (OPENAI_MODEL, "bold bright_white"),
                ),
                title="[bold]Model[/bold]", border_style="bright_blue", box=box.ROUNDED, style="on grey11", padding=(1, 2)
            ))
            continue
        if cmd == "/multiline":
            is_multiline = not is_multiline
            console.print(f"  [dim]Multiline input is now {'ON' if is_multiline else 'OFF'}[/dim]")
            continue
        if cmd == "/metrics":
            print_metrics(sl)
            continue
        if cmd == "/stats":
            uptime = time.time() - session_start
            console.print(Panel(
                Text.assemble(
                    ("Session time", "bright_cyan"), f"  {uptime:.0f}s\n",
                    ("Queries asked", "bright_cyan"), f"  {stats['queries']}\n",
                    ("Errors", "bright_cyan"), f"  {stats['errors']}\n",
                    ("Tokens used", "bright_cyan"), f"  {stats['tokens']:,}\n",
                    ("Avg LLM time", "bright_cyan"), f"  {(stats['llm_time'] / stats['queries']) if stats['queries'] else 0:.1f}s\n",
                    ("Avg SQL time", "bright_cyan"), f"  {(stats['sql_time'] / stats['queries']) if stats['queries'] else 0:.1f}s",
                ),
                title="[bold]Session Stats[/bold]", border_style="bright_cyan", box=box.ROUNDED, style="on grey11", padding=(1, 2)
            ))
            continue
        if cmd.startswith("/export"):
            if not last_result or "columns" not in last_result or "rows" not in last_result:
                console.print(Panel("[red]No query results available to export.[/red]", title="[bold red]Error[/bold red]", border_style="bold red", box=box.HEAVY))
                continue
            parts = question.split(maxsplit=1)
            filename = parts[1] if len(parts) > 1 else "export.csv"
            try:
                with open(filename, 'w', newline='', encoding='utf-8') as f:
                    writer = csv.writer(f)
                    writer.writerow(last_result["columns"])
                    writer.writerows(last_result["rows"])
                console.print(f"  [green]Successfully exported {len(last_result['rows'])} rows to {filename}[/green]")
            except Exception as e:
                console.print(Panel(f"[red]Failed to export: {e}[/red]", title="[bold red]Export Error[/bold red]", border_style="bold red", box=box.HEAVY))
            continue

        explain_mode = False
        if cmd.startswith("/explain "):
            explain_mode = True
            question = question[9:].strip()
            prompt_q = f"Please explain the SQL you would generate for the following request, showing the SQL but also breaking down the reasoning: {question}"
        else:
            prompt_q = question

        stats["queries"] += 1

        console.print(Panel(
            f"[bold white]{question}[/bold white]",
            title="[bold magenta]You[/bold magenta]", title_align="left",
            border_style="magenta", box=box.ROUNDED, padding=(0, 2), expand=False
        ))

        messages.append({"role": "user", "content": prompt_q})

        llm_start = time.time()
        with console.status("[cyan]Generating SQL...[/cyan]", spinner="dots12"):
            try:
                response = client.chat.completions.create(
                    model=OPENAI_MODEL,
                    messages=messages,
                    temperature=0.1,
                )
            except Exception as e:
                stats["errors"] += 1
                console.print(Panel(f"[red]{e}[/red]", title="[bold red]LLM Error[/bold red]", border_style="bold red", box=box.HEAVY))
                continue
        llm_duration = time.time() - llm_start
        stats["llm_time"] += llm_duration

        reply = response.choices[0].message.content
        messages.append({"role": "assistant", "content": reply})
        if hasattr(response, 'usage') and response.usage:
            stats["tokens"] += response.usage.total_tokens

        sql = extract_sql(reply)
        if sql:
            syntax = Syntax(sql, "sql", theme="ansi_dark", line_numbers=False)
            console.print(Panel(syntax, title="[bold green]Generated SQL[/bold green]", title_align="left", border_style="green", box=box.SIMPLE, padding=(0, 1)))

            if explain_mode:
                console.print(Panel(Markdown(reply), title="[bold blue]Explanation[/bold blue]", title_align="left", border_style="blue", box=box.SIMPLE, padding=(0, 1)))
                info = f"  [dim]LLM: {llm_duration:.1f}s[/dim]"
                if hasattr(response, 'usage') and response.usage:
                    info += f"  [dim]{response.usage.total_tokens} tokens[/dim]"
                console.print(info)
                messages[-2]["content"] = question
                console.print(Rule(style="dim"))
                continue

            with console.status("[cyan]Running query...[/cyan]", spinner="dots12"):
                sql_start = time.time()
                result = sl.execute_query(sql)
                sql_duration = time.time() - sql_start
            stats["sql_time"] += sql_duration

            last_result = result

            if "error" in result:
                stats["errors"] += 1
                console.print(Panel(f"[red]{result['error']}[/red]", title="[bold red]SQL Error[/bold red]", border_style="bold red", box=box.HEAVY))
            else:
                console.print()
                print_table(result["columns"], result["rows"])
                plot_results(result["columns"], result["rows"])
                info = f"[dim]{result['row_count']} row{'s' if result['row_count'] != 1 else ''}[/dim]"
                info += f"  [dim]LLM: {llm_duration:.1f}s | SQL: {sql_duration:.1f}s[/dim]"
                if hasattr(response, 'usage') and response.usage:
                    info += f"  [dim]{response.usage.total_tokens} tokens[/dim]"
                console.print(f"  {info}")
        else:
            console.print()
            console.print(Panel(Markdown(reply), title="[bold blue]DataMind[/bold blue]", title_align="left", border_style="blue", box=box.SIMPLE, padding=(0, 1)))
            info = f"  [dim]LLM: {llm_duration:.1f}s[/dim]"
            if hasattr(response, 'usage') and response.usage:
                info += f"  [dim]{response.usage.total_tokens} tokens[/dim]"
            console.print(info)

        console.print(Rule(style="dim"))

    console.print(f"\n  [dim]{stats['queries']} queries answered this session — bye![/dim]\n")


def ask(question):
    sl = SemanticLayer()
    system_prompt = get_system_prompt(sl)
    client_kwargs = {"api_key": OPENAI_API_KEY}
    if OPENAI_BASE_URL:
        client_kwargs["base_url"] = OPENAI_BASE_URL
    client = OpenAI(**client_kwargs)
    messages = [{"role": "system", "content": system_prompt}, {"role": "user", "content": question}]
    response = client.chat.completions.create(model=OPENAI_MODEL, messages=messages, temperature=0.1)
    reply = response.choices[0].message.content
    sql = extract_sql(reply)
    if sql:
        print(f"-- SQL:\n{sql}\n")
        result = sl.execute_query(sql)
        if "error" in result:
            print(f"Error: {result['error']}")
        else:
            print_table(result["columns"], result["rows"])
            print(f"\n{result['row_count']} rows")
    else:
        print(reply)

if __name__ == "__main__":
    if len(sys.argv) > 1:
        ask(" ".join(sys.argv[1:]))
    else:
        chat()
