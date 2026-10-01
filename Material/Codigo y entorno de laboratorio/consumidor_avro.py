#!/usr/bin/env python3
"""Laboratorio 08 - Consumidor Avro. Uso: python3 consumidor_avro.py --group lab08"""
import argparse
from confluent_kafka import Consumer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroDeserializer
from confluent_kafka.serialization import SerializationContext, MessageField

TOPIC = "dian.declaracion.presentada"
ap = argparse.ArgumentParser()
ap.add_argument("--bootstrap", default="localhost:9092")
ap.add_argument("--registry", default="http://localhost:8081")
ap.add_argument("--group", default="lab08")
a = ap.parse_args()

sr = SchemaRegistryClient({"url": a.registry})
# Sin schema_str: el deserializador recupera el esquema del registro usando el ID de los 5 bytes de cabecera
des = AvroDeserializer(sr)
c = Consumer({"bootstrap.servers": a.bootstrap, "group.id": a.group,
              "auto.offset.reset": "earliest", "enable.auto.commit": False})
c.subscribe([TOPIC])
ctx = SerializationContext(TOPIC, MessageField.VALUE)
try:
    n = 0
    while True:
        m = c.poll(2.0)
        if m is None:
            print(f"sin más mensajes ({n} leídos)"); break
        if m.error():
            print(m.error()); continue
        ev = des(m.value(), ctx)
        n += 1
        if n <= 5:
            print(f"p={m.partition()} o={m.offset()} nit={ev['nit']} form={ev['formulario']} "
                  f"valor={ev['valor_pagado']} estado={ev['estado']}")
        c.commit(message=m, asynchronous=False)
finally:
    c.close()
