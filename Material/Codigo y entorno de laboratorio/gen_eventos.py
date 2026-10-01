#!/usr/bin/env python3
"""Generador de eventos sintéticos de facturación electrónica (curso Kafka - DIAN).
Uso: python3 gen_eventos.py --topic dian.facturacion.emitida --n 100
     python3 gen_eventos.py --topic dian.facturacion.emitida --rate 500 --minutes 3
Requiere: pip install confluent-kafka
Datos 100 % sintéticos: los NIT y CUFE no corresponden a contribuyentes reales."""
import argparse, json, random, time, uuid
from datetime import datetime, timedelta, timezone
from confluent_kafka import Producer

# 3 % de "grandes emisores" concentran ~38 % del volumen (sesgo realista)
GRANDES = [f"9{random.randint(10000000, 99999999)}" for _ in range(30)]
PEQUENOS = [f"8{random.randint(10000000, 99999999)}" for _ in range(970)]

def evento(retraso_min=0, valor_total=None):
    nit = random.choice(GRANDES) if random.random() < 0.38 else random.choice(PEQUENOS)
    return nit, {
        "id_evento": str(uuid.uuid4()),
        "nit_emisor": nit,
        "cufe": uuid.uuid4().hex,
        "valor_total": round(random.lognormvariate(12, 1.2), 2) if valor_total is None else valor_total,
        "fecha_emision": (datetime.now(timezone.utc) - timedelta(minutes=retraso_min)).isoformat(),
        "estado": "VALIDADA",
    }

def entregado(err, msg):
    if err is not None:
        print(f"ERROR entregando {msg.key()}: {err}")

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--bootstrap", default="localhost:9092")
    ap.add_argument("--topic", required=True)
    ap.add_argument("--n", type=int, help="número fijo de eventos")
    ap.add_argument("--rate", type=int, help="eventos por segundo (modo continuo)")
    ap.add_argument("--minutes", type=float, default=1)
    ap.add_argument("--retraso-min", type=float, default=0,
                    help="resta minutos a fecha_emision para probar manejo de eventos tardíos")
    ap.add_argument("--valor-total", type=float,
                    help="fija valor_total; use un valor negativo para probar rechazos del Lab14")
    ap.add_argument("--client-id", default="gen-facturacion")
    ap.add_argument("--acks", default="all")
    a = ap.parse_args()
    p = Producer({"bootstrap.servers": a.bootstrap, "client.id": a.client_id, "acks": a.acks,
                  "linger.ms": 20, "compression.type": "lz4"})
    enviados = 0
    if a.n:
        for _ in range(a.n):
            k, v = evento(a.retraso_min, a.valor_total); p.produce(a.topic, key=k, value=json.dumps(v), callback=entregado); enviados += 1
            p.poll(0)
    else:
        fin = time.time() + a.minutes * 60; intervalo = 1.0 / a.rate
        while time.time() < fin:
            t0 = time.time()
            k, v = evento(a.retraso_min, a.valor_total); p.produce(a.topic, key=k, value=json.dumps(v), callback=entregado); enviados += 1
            p.poll(0)
            time.sleep(max(0, intervalo - (time.time() - t0)))
    p.flush()
    print(f"enviados: {enviados}")

if __name__ == "__main__":
    main()
