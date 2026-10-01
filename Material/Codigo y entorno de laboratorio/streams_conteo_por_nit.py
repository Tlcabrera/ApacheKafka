#!/usr/bin/env python3
"""Laboratorio 10 - Equivalente en Python del conteo por NIT y ventana horaria.
NOTA: Kafka Streams es una biblioteca Java. Este script reproduce la MISMA lógica con la API de
consumidor + estado local para que el grupo entienda qué hace Streams por debajo; la versión
oficial del laboratorio es la aplicación Java del repositorio (labs/10-streams/).
Uso: python3 streams_conteo_por_nit.py"""
import json, time
from collections import defaultdict
from datetime import datetime, timezone
from confluent_kafka import Consumer, Producer

ENTRADA, SALIDA = "dian.facturacion.emitida", "dian.facturacion.conteo-por-nit-hora"
GRACIA_S = 600   # periodo de gracia: 10 minutos (parámetro del paso 5)

def ventana(ts_ms):
    dt = datetime.fromtimestamp(ts_ms / 1000, tz=timezone.utc)
    return dt.replace(minute=0, second=0, microsecond=0).isoformat()

c = Consumer({"bootstrap.servers": "localhost:9092", "group.id": "lab10-streams-py",
              "auto.offset.reset": "earliest", "enable.auto.commit": False})
p = Producer({"bootstrap.servers": "localhost:9092", "acks": "all"})
c.subscribe([ENTRADA])
estado = defaultdict(lambda: {"conteo": 0, "valor": 0.0})   # el "state store" local
ultimo_flush = time.time()
try:
    while True:
        m = c.poll(1.0)
        if m is not None and not m.error():
            ev = json.loads(m.value())
            ts = int(datetime.fromisoformat(ev["fecha_emision"]).timestamp() * 1000)
            edad = time.time() - ts / 1000
            if edad > GRACIA_S:
                print(f"evento tardío descartado (edad {edad:.0f}s > gracia {GRACIA_S}s)")
            else:
                k = (ev["nit_emisor"], ventana(ts))
                estado[k]["conteo"] += 1
                estado[k]["valor"] += float(ev["valor_total"])
        if time.time() - ultimo_flush > 10:                  # emisión periódica de resultados
            for (nit, v), agg in estado.items():
                p.produce(SALIDA, key=f"{nit}|{v}", value=json.dumps({"nit": nit, "ventana": v, **agg}))
            p.flush(); ultimo_flush = time.time()
            c.commit(asynchronous=False)
            print(f"emitidas {len(estado)} claves de agregación")
except KeyboardInterrupt:
    pass
finally:
    c.close()
