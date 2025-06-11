#!/usr/bin/env python3
"""
compare_ts_stime.py
===================

Compara los timestamps de Zeek (`ts`) con los de Argus (`Stime`).
Permite tolerancia `--delta` (segundos).

Genera tres ficheros de salida:
* `zeek_unmatched_<delta>.log`    – líneas Zeek sin pareja.
* `argus_unmatched_<delta>.log`   – líneas Argus sin pareja.
* `matched_inexact_<delta>.log`   – pares Zeek‑Argus que coincidieron **dentro de ±delta** pero NO exactamente.

Uso:
====
    python compare_ts_stime.py zeek.jsonl argus.jsonl [--delta 0.001] [--verbose]
"""
from __future__ import annotations
import argparse, bisect, json, logging, os, sys, math
from typing import List, Tuple

# ---------------------------------------------------------------------------
# Helper: cargar Argus ordenado
# ---------------------------------------------------------------------------

def load_argus(path: str) -> Tuple[List[float], List[str]]:
    ts_list: List[float] = []
    lines:   List[str]   = []
    with open(path, "r", encoding="utf-8") as fh:
        for line in fh:
            line = line.rstrip("\n")
            if not line:
                continue
            try:
                rec = json.loads(line)
                raw = rec.get("Stime") or rec.get("stime")
                if raw is None:
                    continue
                ts = float(raw)
                idx = bisect.bisect_right(ts_list, ts)
                ts_list.insert(idx, ts)
                lines.insert(idx, line)
            except (json.JSONDecodeError, ValueError):
                logging.debug("Línea Argus inválida; ignorada.")
    return ts_list, lines

# ---------------------------------------------------------------------------
# Búsqueda con tolerancia (bisect)
# ---------------------------------------------------------------------------

def find_match(ts_list: List[float], target: float, delta: float) -> int | None:
    left = bisect.bisect_left(ts_list, target - delta)
    right = bisect.bisect_right(ts_list, target + delta)
    return left if left < right else None

# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("zeek_file")
    ap.add_argument("argus_file")
    ap.add_argument("--delta", type=float, default=0.0,
                    help="Tolerancia en segundos (ej. 0.001 = 1 ms)")
    ap.add_argument("--verbose", action="store_true")
    args = ap.parse_args()

    logging.basicConfig(level=logging.DEBUG if args.verbose else logging.INFO,
                        format="%(levelname)s %(message)s")

    if not os.path.isfile(args.zeek_file) or not os.path.isfile(args.argus_file):
        sys.exit("❌  Archivos no encontrados.")

    # Cargar Argus
    logging.info("Indexando Argus…")
    argus_ts, argus_lines = load_argus(args.argus_file)
    total_argus = len(argus_ts)

    matched = 0
    matched_inexact = 0

    zu_path = f"zeek_unmatched_{args.delta}.log"
    au_path = f"argus_unmatched_{args.delta}.log"
    mi_path = f"matched_inexact_{args.delta}.log"

    zeek_unmatched  = open(zu_path, "w", encoding="utf-8")
    matched_inexact_fh = open(mi_path, "w", encoding="utf-8")

    # Procesar Zeek
    total_zeek = 0
    with open(args.zeek_file, "r", encoding="utf-8") as zfh:
        for z_line in zfh:
            z_line = z_line.rstrip("\n")
            if not z_line:
                continue
            total_zeek += 1
            try:
                z_rec = json.loads(z_line)
                ts = float(z_rec.get("ts"))
            except (json.JSONDecodeError, TypeError, ValueError):
                logging.debug("Línea Zeek inválida")
                continue

            idx = find_match(argus_ts, ts, args.delta)
            if idx is not None:
                matched += 1
                arg_line = argus_lines.pop(idx)
                arg_ts   = argus_ts.pop(idx)
                # comprobar si fue coincidencia exacta
                if not math.isclose(ts, arg_ts, rel_tol=0.0, abs_tol=0.0):
                    matched_inexact += 1
                    matched_inexact_fh.write("Z:" + z_line + "\nA:" + arg_line + "\n---\n")
            else:
                zeek_unmatched.write(z_line + "\n")

    zeek_unmatched.close()
    matched_inexact_fh.close()

    # Argus restantes → sin pareja
    with open(au_path, "w", encoding="utf-8") as fh:
        for l in argus_lines:
            fh.write(l + "\n")

    print(f"\nResumen (delta={args.delta}s)")
    print(f"  Zeek totales      : {total_zeek}")
    print(f"  Argus totales     : {total_argus}")
    print(f"  Coincidencias     : {matched}")
    print(f"    └ inexactas (+/-δ): {matched_inexact} → {mi_path}")
    print(f"  Zeek sin pareja   : {total_zeek - matched} → {zu_path}")
    print(f"  Argus sin pareja  : {total_argus - matched} → {au_path}")

if __name__ == "__main__":
    main()
