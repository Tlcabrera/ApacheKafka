#!/usr/bin/env python3
"""Laboratorio 05 - Esqueleto del productor de eventos de facturación.
Complete las secciones marcadas con TODO. Uso: python3 productor_facturacion.py --acks all --linger 20 --compression lz4 --n 5000"""
import argparse, json, time, uuid, random
from confluent_kafka import Producer, KafkaError

def construir_evento():
    nit = f"9{random.randint(10000000, 99999999)}"
    return nit, {"id_evento": str(uuid.uuid4()), "nit_emisor": nit, "cufe": uuid.uuid4().hex,
                 "valor_total": round(random.uniform(10000, 5000000), 2), "estado": "VALIDADA"}

errores = 0
def entregado(err, msg):
    """Callback de entrega. TODO (paso 4): distinguir errores reintentables de no reintentables
    y registrar partición y offset en los envíos exitosos."""
    global errores
    if err is not None:
        errores += 1
        print(f"[ERROR] clave={msg.key()} codigo={err.code()} reintentable={err.retriable()} -> {err}")
    # else: print(f"ok p={msg.partition()} o={msg.offset()}")

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--bootstrap", default="localhost:9092")
    ap.add_argument("--topic", default="lab05.facturacion.emitida")
    ap.add_argument("--n", type=int, default=5000)
    ap.add_argument("--acks", default="all")            # 0 | 1 | all
    ap.add_argument("--linger", type=int, default=0)     # ms
    ap.add_argument("--compression", default="none")     # none | snappy | lz4 | zstd | gzip
    ap.add_argument("--idempotence", action="store_true")
    a = ap.parse_args()

    conf = {
        "bootstrap.servers": a.bootstrap,
        "client.id": "lab05-productor",
        "acks": a.acks,
        "linger.ms": a.linger,
        "compression.type": a.compression,
        "enable.idempotence": a.idempotence,
        # TODO (paso 1): agregue batch.size y delivery.timeout.ms y justifique los valores en el informe
    }
    if a.idempotence:
        conf["acks"] = "all"   # la idempotencia exige acks=all
    p = Producer(conf)

    t0 = time.time()
    for _ in range(a.n):
        k, v = construir_evento()
        # TODO (paso 1): la clave debe ser el NIT para conservar el orden por contribuyente
        p.produce(a.topic, key=k, value=json.dumps(v), callback=entregado)
        p.poll(0)   # sirve los callbacks pendientes
    p.flush()
    dt = time.time() - t0
    print(f"acks={a.acks} linger={a.linger} compression={a.compression} idempotence={a.idempotence} "
          f"-> {a.n} eventos en {dt:.2f}s = {a.n/dt:.0f} ev/s, errores={errores}")

if __name__ == "__main__":
    main()
