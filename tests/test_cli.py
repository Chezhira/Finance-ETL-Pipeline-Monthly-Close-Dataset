import subprocess
import sys

def _run(cmd):
    return subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True)

def _cli(*args):
    """Try console script first; if not available, fall back to module."""
    res = _run(["finance-etl", *args])
    if res.returncode != 0:
        res = _run([sys.executable, "-m", "finance_etl.cli", *args])
    return res

def test_cli_run_help_has_options():
    res = _cli("run", "--help")
    out = res.stdout
    assert res.returncode == 0
    assert "--month" in out
    assert "--fail-on" in out

def test_cli_version_command():
    res = _cli("version")
    out = res.stdout
    assert res.returncode == 0
    assert "finance-etl" in out  # expects "finance-etl 0.1.0"
