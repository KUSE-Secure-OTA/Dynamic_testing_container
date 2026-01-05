#!/usr/bin/env bash
set -euo pipefail

KEY_FILE=""
VEC_FILE=""

while [[ $# -gt 0 ]]; do
  case "$1" in
    --key) KEY_FILE="$2"; shift 2;;
    --vector) VEC_FILE="$2"; shift 2;;
    *) echo "unknown arg: $1" >&2; exit 2;;
  esac
done

if [[ -z "${KEY_FILE}" || -z "${VEC_FILE}" ]]; then
  echo "usage: verify-probe --key <file> --vector <file>" >&2
  exit 2
fi

key="$(tr -d '\r\n ' < "$KEY_FILE" || true)"
expected="$(grep -E '^EXPECTED_KEY=' "$VEC_FILE" | head -n1 | cut -d= -f2- | tr -d '\r\n ' || true)"

if [[ -z "$key" || -z "$expected" ]]; then
  echo "invalid key/vector format" >&2
  exit 2
fi

if [[ "$key" == "$expected" ]]; then
  echo "PASS (key=$key expected=$expected)"
  exit 0
else
  echo "FAIL (key=$key expected=$expected)" >&2
  exit 1
fi
