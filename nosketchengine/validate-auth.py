"""Reject missing or weak authentication records without logging their contents."""

import re
import sys
from pathlib import Path


def main():
    # htpasswd -n adds a blank line after its record; preserve strict validation
    # within the file while accepting those trailing line endings.
    records = Path(sys.argv[1]).read_bytes().rstrip(b"\r\n").splitlines()
    pattern = re.compile(
        rb"([A-Za-z0-9_.@-]+):\$2[aby]\$(1[0-6])\$[./A-Za-z0-9]{53}"
    )
    users = set()
    if not records:
        sys.exit("NoSketch Engine authentication requires at least one user.")
    for record in records:
        match = pattern.fullmatch(record)
        if match is None or match[1] in users:
            sys.exit(
                "Invalid NOSKE_HTPASSWD: use unique usernames and bcrypt hashes "
                "with cost 10–16 (htpasswd -nB -C 12 USERNAME)."
            )
        users.add(match[1])


if __name__ == "__main__":
    main()
