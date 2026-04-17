"""Process metric collection helpers used by the publisher."""

import psutil


def get_metrics_for_app(app_name: str) -> str:
    """Return CPU and memory usage for the first process whose name contains app_name, or None if no match exists."""
    for proc in psutil.process_iter(["name", "status"]):
        try:
            proc_name = proc.info["name"] or ""
            if app_name.lower() in proc_name.lower():
                cpu = proc.cpu_percent(interval=0.1)
                mem = proc.memory_percent()
                return f"cpu={cpu:.1f} memory={mem:.2f}"
        except (psutil.NoSuchProcess, psutil.AccessDenied):
            continue
    return None


def get_system_metrics() -> str:
    """Return system-wide CPU and memory usage as a fallback measurement."""
    cpu = psutil.cpu_percent(interval=0.1)
    mem = psutil.virtual_memory().percent
    return f"cpu={cpu:.1f} memory={mem:.1f}"


def list_running_processes() -> list:
    """Return sorted (pid, name) tuples for running processes to aid diagnostics."""
    processes = []
    for proc in psutil.process_iter(["pid", "name"]):
        try:
            processes.append((proc.info["pid"], proc.info["name"]))
        except (psutil.NoSuchProcess, psutil.AccessDenied):
            continue
    return sorted(processes, key=lambda item: item[1].lower())
