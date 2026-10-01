#!/usr/bin/env python3
"""Laboratorio 05 - Ejecuta la matriz de mediciones y produce una tabla Markdown para el informe.
Uso: python3 bench.py --topic equipo01.lab05.facturacion.emitida --n 20000"""
import argparse, itertools, subprocess, re, sys
from pathlib import Path
ap = argparse.ArgumentParser()
ap.add_argument("--topic", default="lab05.facturacion.emitida")
ap.add_argument("--n", type=int, default=20000)
a = ap.parse_args()

def correr(args):
    productor = Path(__file__).with_name("productor_facturacion.py")
    out = subprocess.run([sys.executable, str(productor), "--topic", a.topic, "--n", str(a.n)] + args, capture_output=True, text=True).stdout
    m = re.search(r"= (\d+) ev/s", out); return int(m.group(1)) if m else -1

print("## Paso 2: acks\n| acks | ev/s |\n|---|---|")
for acks in ["0", "1", "all"]:
    print(f"| {acks} | {correr(['--acks', acks])} |")

print("\n## Paso 3: linger.ms × compression.type (acks=all)\n| linger.ms | compression | ev/s |\n|---|---|---|")
for linger, comp in itertools.product([0, 20, 100], ["none", "snappy", "zstd"]):
    print(f"| {linger} | {comp} | {correr(['--acks', 'all', '--linger', str(linger), '--compression', comp])} |")
print(f"\nTamaño en disco por combinación: docker exec broker1 du -sh /var/lib/kafka/data/{a.topic}-*")
