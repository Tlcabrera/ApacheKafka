# Laboratorio 09 — Ingesta y descarga sin código (Connect, SMT y CDC)
**Sesión 12** · Módulo 5.3 · 40 min guiados en vivo · Informe con tabla comparativa (martes 03/11)

## Objetivo
Configurar un source JDBC con converter Avro y SMT, un sink a MinIO particionado por fecha, sustituirlo por Debezium y comparar qué captura cada enfoque; provocar la redistribución de tareas.

## Prerrequisitos
Etapa C en ejecución en AWS/code-server (incluye PostgreSQL con 500 declaraciones semilla y MinIO). Desde el navegador, las interfaces se consultan con la IP pública; desde la terminal integrada, `localhost` apunta a la EC2 y es válido para Connect y Schema Registry.
Antes de la clase, el docente debe construir o cargar la imagen de Connect
versionada; vea `entorno/connect/README.md`. Verifique los plugins:
```bash
cd ~/kafka-dian
curl -s localhost:8083/connector-plugins | python3 -c "import json,sys;[print(p['class']) for p in json.load(sys.stdin)]" | grep -Ei "jdbc|s3|postgres"
```
`minio-init` crea el bucket `dian-bronce`. Verifique que terminó correctamente:

```bash
cd entorno
docker compose -f docker-compose-lab-kafka.yml -f docker-compose.etapaC.yml ps -a minio-init
cd ..
```

## Paso 1 — Source JDBC incremental ★
```bash
curl -s -X POST -H "Content-Type: application/json" --data @codigo/conector-jdbc-source.json localhost:8083/connectors
sleep 20
curl -s localhost:8083/connectors/declaraciones-jdbc-source/status | python3 -m json.tool
docker exec broker1 /opt/kafka/bin/kafka-topics.sh --bootstrap-server broker1:19092 --list | grep jdbc
```
**Salida esperada:** estado `RUNNING`, tarea `RUNNING`, y el topic `jdbc.declaraciones`.

## Paso 2 — Verificar el esquema derivado automáticamente ★
```bash
curl -s localhost:8081/subjects | python3 -m json.tool
curl -s localhost:8081/subjects/jdbc.declaraciones-value/versions/1 | python3 -m json.tool | head -25
```
**Punto clave:** nadie escribió este esquema. Connect lo derivó de la tabla y lo registró. El flujo tiene contrato desde el primer día.

## Paso 3 — Efecto de las SMT ★
```bash
docker exec broker1 /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server broker1:19092 \
  --topic jdbc.declaraciones --from-beginning --max-messages 3 --property print.key=true 2>/dev/null | head
```
**Observe:** la clave es el NIT (`ValueToKey` + `ExtractField`) y el campo `nit` del valor aparece como `SEUDONIMIZADO` (`MaskField`). Un mismo NIT cae siempre en la misma partición aunque su valor esté enmascarado.
**Para el informe:** ¿qué implicación de cumplimiento tiene enmascarar en tránsito antes de que el dato llegue al destino analítico?

## Paso 4 — Sink hacia MinIO ★
```bash
curl -s -X POST -H "Content-Type: application/json" --data @codigo/conector-s3-sink.json localhost:8083/connectors
sleep 60
bash entorno/minio.sh ls -r local/dian-bronce | head
```
**Salida esperada:** objetos bajo `topics/jdbc.declaraciones/fecha=2026-.../` en formato Parquet.

## Paso 5 — JDBC frente a CDC ★ (núcleo del laboratorio)
Con el source JDBC activo, ejecute en PostgreSQL:
```bash
docker exec -it postgres psql -U dian -c "UPDATE declaraciones SET valor_pagado = valor_pagado + 1, actualizado_en = now() WHERE id_declaracion = 1;"
docker exec -it postgres psql -U dian -c "DELETE FROM declaraciones WHERE id_declaracion = 2;"
```
Espere 15 s y cuente los mensajes nuevos en `jdbc.declaraciones`. **Observe:** el `UPDATE` aparece (cambió `actualizado_en`); el `DELETE` **no aparece en absoluto**.

Ahora despliegue Debezium:
```bash
curl -s -X POST -H "Content-Type: application/json" --data @codigo/conector-debezium-source.json localhost:8083/connectors
sleep 45
docker exec -it postgres psql -U dian -c "UPDATE declaraciones SET estado='CORREGIDA', actualizado_en=now() WHERE id_declaracion = 3;"
docker exec -it postgres psql -U dian -c "DELETE FROM declaraciones WHERE id_declaracion = 4;"
docker exec broker1 /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server broker1:19092 \
  --topic cdc.public.declaraciones --from-beginning --timeout-ms 15000 --property print.key=true 2>/dev/null | tail -5
```
**Observe:** el `DELETE` sí aparece, con el campo `__deleted=true` (por la SMT `ExtractNewRecordState` en modo `rewrite`), y el `snapshot.mode=initial` cargó primero toda la tabla. Verifique la ranura de replicación:
```bash
docker exec -it postgres psql -U dian -c "SELECT slot_name, active FROM pg_replication_slots;"
```

## Paso 6 — Redistribución de tareas
```bash
curl -s localhost:8083/connectors/declaraciones-s3-sink/status | python3 -m json.tool | grep -A2 tasks
docker restart connect && sleep 40
curl -s localhost:8083/connectors | python3 -m json.tool
```
**Observe:** los conectores reaparecen sin volver a crearlos: su configuración vivía en `connect-configs`, y la posición de lectura en `connect-offsets`.

## Informe
Tabla comparativa con evidencia propia:

| Dimensión | JDBC incremental | CDC (Debezium) |
|---|---|---|
| Latencia observada | | |
| ¿Detectó el DELETE? | | |
| Carga sobre el origen | | |
| Requisitos previos | | |

Más: (a) implicación de cumplimiento del enmascaramiento en tránsito; (b) ¿qué área de la DIAN debe autorizar los permisos de replicación que exige Debezium y qué le argumentaría?; (c) un flujo de su dependencia donde usaría CDC y por qué.
