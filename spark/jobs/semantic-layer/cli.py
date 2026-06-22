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
]


def print_logo():
    console.clear()
    console.print()
    console.print(LOGO, highlight=False)
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


def print_table(columns, rows):
    table = Table(
        box=box.SIMPLE_HEAVY,
        border_style="cyan",
        header_style="bold cyan",
        show_edge=False,
        padding=(0, 2),
    )
    for col in columns:
        table.add_column(col)
    for row in rows:
        table.add_row(*[str(c) if c is not None else "NULL" for c in row])
    if rows:
        console.print(table)
    else:
        console.print("  [dim]no results[/dim]")


def print_help():
    console.print(Panel(
        "[bold]Commands[/bold]\n"
        "[cyan]/exit[/cyan]      [dim]Exit[/dim]\n"
        "[cyan]/clear[/cyan]     [dim]Clear screen[/dim]\n"
        "[cyan]/context[/cyan]   [dim]Show full semantic context[/dim]\n"
        "[cyan]/metrics[/cyan]   [dim]List all metrics[/dim]\n"
        "[cyan]/examples[/cyan]  [dim]Show example questions[/dim]\n"
        "[cyan]/model[/cyan]     [dim]Show model info[/dim]",
        title="Help", border_style="cyan", box=box.SIMPLE,
    ))


def print_examples(sl):
    table = Table(box=box.SIMPLE, show_header=False, border_style="dim", padding=(0, 1))
    table.add_column("", style="cyan", width=4)
    table.add_column("", style="white")
    for m in sl.get_metrics_list()[:8]:
        table.add_row(">", f"{m['label']}")
    console.print(Panel(table, title="Examples - ask about", border_style="dim", box=box.SIMPLE))


def print_metrics(sl):
    table = Table(box=box.SIMPLE, show_header=False, border_style="dim", padding=(0, 1))
    table.add_column("", style="cyan")
    table.add_column("", style="white")
    table.add_column("", style="dim")
    for m in sl.get_metrics_list():
        table.add_row(m["name"], m["label"], m.get("table", ""))
    console.print(table)


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
            ("metrics", "cyan"), "  ", (f"{len(sl.metrics)}", "bold white"), "  |  ",
            ("dimensions", "cyan"), "  ", (f"{len(sl.dimensions)}", "bold white"), "  |  ",
            ("entities", "cyan"), "  ", (f"{len(sl.entities)}", "bold white"), "  |  ",
            ("relationships", "cyan"), "  ", (f"{len(sl.relationships)}", "bold white"),
        ),
        box=box.SIMPLE, border_style="dim", padding=(0, 1),
    ))

    system_prompt = get_system_prompt(sl)

    client_kwargs = {"api_key": OPENAI_API_KEY}
    if OPENAI_BASE_URL:
        client_kwargs["base_url"] = OPENAI_BASE_URL
    client = OpenAI(**client_kwargs)

    messages = [{"role": "system", "content": system_prompt}]

    try:
        import readline
        try:
            readline.read_history_file(str(HISTORY_FILE))
        except FileNotFoundError:
            pass
        _has_readline = True
    except ModuleNotFoundError:
        _has_readline = False

    while True:
        try:
            question = Prompt.ask("[cyan]>[/cyan]")
        except (EOFError, KeyboardInterrupt):
            console.print("\n[dim]bye![/dim]")
            break

        question = question.strip()
        if not question:
            continue

        if _has_readline:
            try:
                readline.write_history_file(str(HISTORY_FILE))
            except Exception:
                pass

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
            console.print(
                f"  Provider  [dim]{OPENAI_BASE_URL or 'https://api.openai.com/v1'}[/dim]\n"
                f"  Model     [cyan]{OPENAI_MODEL}[/cyan]"
            )
            continue
        if cmd == "/metrics":
            print_metrics(sl)
            continue

        messages.append({"role": "user", "content": question})

        with console.status("[cyan]>[/cyan] thinking...", spinner="dots"):
            try:
                response = client.chat.completions.create(
                    model=OPENAI_MODEL,
                    messages=messages,
                    temperature=0.1,
                )
            except Exception as e:
                console.print(f"\n  [red]![/red] {e}")
                continue

        reply = response.choices[0].message.content
        messages.append({"role": "assistant", "content": reply})

        sql = extract_sql(reply)
        if sql:
            syntax = Syntax(sql, "sql", theme="ansi_dark", line_numbers=False)
            console.print(Panel(syntax, border_style="green", box=box.SIMPLE, padding=(0, 1)))

            result = sl.execute_query(sql)
            if "error" in result:
                console.print(f"\n  [red]![/red] {result['error']}")
            else:
                console.print()
                print_table(result["columns"], result["rows"])
                info = f"[dim]{result['row_count']} row{'s' if result['row_count'] != 1 else ''}[/dim]"
                if hasattr(response, 'usage') and response.usage:
                    info += f"  [dim]{response.usage.total_tokens} tokens[/dim]"
                console.print(f"  {info}")
        else:
            console.print()
            console.print(Panel(Markdown(reply), border_style="blue", box=box.SIMPLE, padding=(0, 1)))

        console.print("  [dim]" + "-" * 50 + "[/dim]")

    console.print("  [dim]" + "-" * 50 + "[/dim]")


if __name__ == "__main__":
    chat()
