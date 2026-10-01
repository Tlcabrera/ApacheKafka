#!/usr/bin/env python3
"""Laboratorio 06 - Esqueleto del consumidor con commit manual, persistencia en PostgreSQL y DLQ.
Uso: python3 consumidor_facturacion.py --group facturacion-sink [--upsert] [--kill-after 60]
Requiere: pip install confluent-kafka psycopg2-binary"""
import argparse, json, sys, time
import psycopg2
from confluent_kafka import Consumer, Producer, KafkaException

DDL = """CREATE TABLE IF NOT EXISTS facturas (
  id_evento TEXT PRIMARY KEY, nit_emisor TEXT, cufe TEXT, valor_total NUMERIC, estado TEXT,
  particion INT, "offset" BIGINT, procesado_en TIMESTAMPTZ DEFAULT now());"""
INSERT = "INSERT INTO facturas (id_evento,nit_emisor,cufe,valor_total,estado,particion,\"offset\") VALUES (%s,%s,%s,%s,%s,%s,%s)"
UPSERT = INSERT + " ON CONFLICT (id_evento) DO UPDATE SET estado=EXCLUDED.estado, procesado_en=now()"

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--bootstrap", default="localhost:9092")
    ap.add_argument("--topic", default="lab05.facturacion.emitida")
    ap.add_argument("--group", default="facturacion-sink")
    ap.add_argument("--pg", default="dbname=dian user=dian password=dian host=localhost port=5432")
    ap.add_argument("--upsert", action="store_true", help="paso 3: idempotencia por id_evento")
    ap.add_argument("--kill-after", type=int, default=0, help="paso 2: salir abruptamente tras N mensajes, ANTES del commit")
    ap.add_argument("--max-reintentos", type=int, default=3)
    a = ap.parse_args()

    c = Consumer({"bootstrap.servers": a.bootstrap, "group.id": a.group, "enable.auto.commit": False,
                  "auto.offset.reset": "earliest", "max.poll.interval.ms": 300000,
                  "partition.assignment.strategy": "cooperative-sticky"})
    dlq = Producer({"bootstrap.servers": a.bootstrap, "acks": "all"})
    pg = psycopg2.connect(a.pg); pg.autocommit = False
    with pg.cursor() as cur: cur.execute(DDL); pg.commit()
    c.subscribe([a.topic])
    procesados = 0
    try:
        while True:
            msg = c.poll(1.0)
            if msg is None: continue
            if msg.error(): raise KafkaException(msg.error())
            ok = False
            for intento in range(1, a.max_reintentos + 1):
                try:
                    ev = json.loads(msg.value())                      # falla si el mensaje está malformado
                    with pg.cursor() as cur:
                        cur.execute(UPSERT if a.upsert else INSERT,
                                    (ev["id_evento"], ev["nit_emisor"], ev["cufe"], ev["valor_total"], ev["estado"],
                                     msg.partition(), msg.offset()))
                    pg.commit(); ok = True; break
                except Exception as e:
                    pg.rollback()
                    print(f"[intento {intento}] p={msg.partition()} o={msg.offset()} error={type(e).__name__}: {e}")
                    time.sleep(0.5 * intento)
            if not ok:
                # TODO (paso 6): patrón DLQ - publicar con encabezados de contexto y avanzar
                dlq.produce(a.topic + ".dlq", key=msg.key(), value=msg.value(), headers={
                    "origen.topic": msg.topic(), "origen.particion": str(msg.partition()),
                    "origen.offset": str(msg.offset()), "error": "procesamiento fallido tras reintentos",
                    "consumidor": "lab06-v1", "ts": str(int(time.time()))})
                dlq.flush()
                print(f"-> enviado a DLQ p={msg.partition()} o={msg.offset()}")
            procesados += 1
            if a.kill_after and procesados >= a.kill_after:
                print(f"Simulando caída tras {procesados} mensajes, SIN commit"); sys.exit(1)
            c.commit(message=msg, asynchronous=False)   # commit manual después de procesar
    except KeyboardInterrupt:
        pass
    finally:
        c.close(); pg.close()

if __name__ == "__main__":
    main()
