#!/usr/bin/env python3
"""Entry point delegating to :mod:`cli` module."""
from __future__ import annotations

import sys

from cli import main as cli_main


def main() -> int:
    return cli_main()


if __name__ == "__main__":  # pragma: no cover
    sys.exit(main())
