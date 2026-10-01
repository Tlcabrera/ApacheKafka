#!/usr/bin/env python3
"""Laboratorio 08 - Productor con Avro y Schema Registry.
Uso: python3 productor_avro.py --n 50 [--invalido]
Requiere: pip install "confluent-kafka[avro,schemaregistry]" """
import argparse, json, random, uuid
from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path
from confluent_kafka import Producer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer
from confluent_kafka.serialization import StringSerializer, SerializationContext, MessageField

TOPIC = "dian.declaracion.presentada"

def a_dict(obj, ctx):
    return obj

def construir(invalido=False):
    nit = ("9" if random.random() < 0.4 else "8") + f"{random.randint(0, 99999999):08d}"
    ev = {
        "id_evento": str(uuid.uuid4()),
        "nit": nit,
        "formulario": random.choice(["110", "300", "350", "490"]),
        "periodo": f"2026-{random.randint(1,9):02d}",
        "fecha_presentacion": int(datetime.now(timezone.utc).timestamp() * 1000),
        "valor_pagado": Decimal(f"{random.randint(10000, 50000000)}.00"),
        "estado": random.choice(["PRESENTADA", "CORREGIDA"]),
    }
    if invalido:
        ev["estado"] = "EN_TRAMITE"      # símbolo que NO existe en el enum: el serializador lo rechaza
    return nit, ev

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--bootstrap", default="localhost:9092")
    ap.add_argument("--registry", default="http://localhost:8081")
    ap.add_argument("--schema", default=str(Path(__file__).with_name("declaracion-presentada-v1.avsc")))
    ap.add_argument("--n", type=int, default=50)
    ap.add_argument("--invalido", action="store_true", help="paso 6: producir un evento que viola el esquema")
    a = ap.parse_args()

    sr = SchemaRegistryClient({"url": a.registry})
    with open(a.schema) as f:
        esquema = f.read()
    ser = AvroSerializer(sr, esquema, a_dict)
    kser = StringSerializer("utf_8")
    p = Producer({"bootstrap.servers": a.bootstrap, "acks": "all", "enable.idempotence": True})

    ctx = SerializationContext(TOPIC, MessageField.VALUE)
    enviados = 0
    for _ in range(a.n):
        k, ev = construir(a.invalido)
        try:
            p.produce(TOPIC, key=kser(k), value=ser(ev, ctx))   # la validación ocurre AQUÍ, antes de la red
            enviados += 1
        except Exception as e:
            print(f"[RECHAZADO POR EL ESQUEMA] {type(e).__name__}: {e}")
            break
        p.poll(0)
    p.flush()
    print(f"enviados: {enviados}")

if __name__ == "__main__":
    main()
