"""Terminal presentation helpers.

This module only changes how runtime information is displayed; pipeline logic
and returned values remain untouched.
"""


def banner(title, subtitle=None):
    line = "═" * 68
    print(f"\n╔{line}╗")
    print(f"║ {title.center(66)} ║")
    if subtitle:
        print(f"║ {subtitle.center(66)} ║")
    print(f"╚{line}╝")


def section(title):
    print(f"\n┌─ {title} {'─' * max(3, 62 - len(title))}┐")


def info(label, message):
    print(f"  ◆ {label:<14} {message}")


def success(message):
    print(f"  ✓ {message}")


def warning(message):
    print(f"  ! {message}")


def error(message):
    print(f"  ✗ {message}")


def metric(label, value):
    print(f"  • {label:<30} {value}")
