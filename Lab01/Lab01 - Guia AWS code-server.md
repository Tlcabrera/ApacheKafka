# Laboratorio 01 en AWS/code-server — Anatomía de un clúster de tres brokers

## Propósito

Este documento adapta el Lab 01 al ambiente AWS del curso. A diferencia de la guía original local, aquí los estudiantes no instalan herramientas en sus equipos corporativos: ingresan por navegador a code-server y ejecutan los comandos dentro de una instancia EC2 preparada.

## Qué replica este laboratorio

El laboratorio replica un escenario básico de alta disponibilidad en Kafka:

- Tres brokers Kafka en modo KRaft.
- Topics con factor de replicación `3`.
- `min.insync.replicas=2`.
- Escrituras que continúan aunque caiga un broker.
- Cambio de líder de partición cuando el broker líder deja de estar disponible.
- Reincorporación del broker al ISR cuando vuelve a estar activo.

La idea central es observar que Kafka no guarda un mensaje en un solo servidor. Cada partición tiene réplicas distribuidas entre brokers; una réplica actúa como líder y las demás siguen el log. El ISR representa las réplicas sincronizadas que pueden garantizar durabilidad.

## Antes de iniciar

Ingrese al code-server de su equipo y abra una terminal integrada.

Cada equipo trabaja en su propio workspace:

```text
/home/equipoXX/kafka-dian
```

El docente trabaja en:

```text
/home/docente/kafka-dian
```

## Paso 1 — Ubicarse en el ambiente Kafka

Desde la terminal integrada:

```bash
cd ~/kafka-dian/entorno
ls
```

Debe existir el archivo:

```text
docker-compose-lab-kafka.yml
```

## Paso 2 — Levantar el clúster Kafka

Solo debe haber un grupo levantando el clúster compartido a la vez. En sesión guiada, lo normal es que lo haga el docente o el equipo asignado por el docente.

```bash
docker compose -f docker-compose-lab-kafka.yml up -d
```

Verifique estado:

```bash
docker compose -f docker-compose-lab-kafka.yml ps
```

Salida esperada:

- `broker1`, `broker2`, `broker3` en ejecución.
- `kafka-ui` en ejecución.
- `bootstrap-topics` finalizado correctamente o creado los topics base.

## Paso 3 — Abrir Kafka UI

Desde el navegador:

```text
http://35.153.139.120:8080
```

Verifique:

- Clúster `dian-lab`.
- Tres brokers visibles.
- Topics base `dian.*`.

## Paso 4 — Definir función auxiliar para comandos Kafka

En la terminal:

```bash
KB=broker1
kt() { docker exec "$KB" /opt/kafka/bin/kafka-topics.sh --bootstrap-server "$KB":19092 "$@"; }
```

Verifique:

```bash
kt --list
```

## Paso 5 — Verificar brokers del clúster

```bash
docker exec broker1 /opt/kafka/bin/kafka-broker-api-versions.sh --bootstrap-server broker1:19092 | grep -E "^broker[0-9]"
```

Guarde esta salida para el informe.

## Paso 6 — Crear topic replicado

Use un topic por equipo para evitar colisiones. Reemplace `equipoXX` por su equipo.

Ejemplo para `equipo01`:

```bash
TOPIC=equipo01.lab01.facturacion.emitida
```

Cree el topic:

```bash
kt --create --topic "$TOPIC" --partitions 3 --replication-factor 3 --config min.insync.replicas=2
```

Liste topics:

```bash
kt --list | sort
```

## Paso 7 — Describir líder, réplicas e ISR

```bash
kt --describe --topic "$TOPIC"
```

Identifique para la partición `0`:

- `Leader`
- `Replicas`
- `Isr`

Anote qué broker es líder de la partición 0. Ese será el broker que se detendrá más adelante.

## Paso 8 — Producir 100 eventos

Desde `~/kafka-dian/entorno`, use el generador canónico:

```bash
python3 ../codigo/gen_eventos.py --topic "$TOPIC" --n 100
```

Si el generador Python falla, use esta alternativa:

```bash
for i in $(seq 1 100); do echo "900${i}|{\"nit\":\"900${i}\",\"valor\":$((RANDOM*10)),\"n\":$i}"; done | \
docker exec -i broker1 /opt/kafka/bin/kafka-console-producer.sh --bootstrap-server broker1:19092 \
  --topic "$TOPIC" --property parse.key=true --property key.separator="|"
```

## Paso 9 — Inspeccionar archivos físicos del log

Reemplace `brokerN` por el broker líder de la partición 0 identificado en el paso 7.

```bash
docker exec brokerN ls -lh /var/lib/kafka/data/${TOPIC}-0/
```

Salida esperada:

```text
00000000000000000000.index
00000000000000000000.log
00000000000000000000.timeindex
leader-epoch-checkpoint
partition.metadata
```

Ver contenido del segmento:

```bash
docker exec brokerN /opt/kafka/bin/kafka-dump-log.sh --print-data-log \
  --files /var/lib/kafka/data/${TOPIC}-0/00000000000000000000.log | head -20
```

Observe:

- `offset`
- `CreateTime`
- `key`
- `payload`

## Paso 10 — Provocar caída del líder

Este paso afecta el clúster compartido. Debe hacerse solo bajo instrucción del docente.

Detenga el broker líder de la partición 0:

```bash
docker stop brokerN
```

Espere:

```bash
sleep 10
```

Si detuvo `broker1`, cambie el broker usado por la función `kt`:

```bash
KB=broker2
```

Describa de nuevo:

```bash
kt --describe --topic "$TOPIC"
```

Debe observar:

- Nuevo líder para la partición 0.
- ISR reducido de tres brokers a dos.
- Kafka sigue disponible porque quedan dos réplicas sincronizadas y `min.insync.replicas=2`.

## Paso 11 — Producir 10 eventos adicionales

```bash
python3 ../codigo/gen_eventos.py --topic "$TOPIC" --n 10
```

La escritura debe funcionar si el ISR conserva dos réplicas.

## Paso 12 — Reiniciar broker y verificar recuperación

```bash
docker start brokerN
sleep 20
kt --describe --topic "$TOPIC"
```

Debe observar que el ISR vuelve a incluir tres brokers.

## Paso 13 — Contar mensajes

```bash
docker exec broker1 /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server broker1:19092 \
  --topic "$TOPIC" --from-beginning --timeout-ms 10000 2>/dev/null | wc -l
```

Salida esperada:

```text
110
```

## Evidencias para el informe

El informe debe incluir:

1. Salida del paso 5.
2. Salida del paso 7.
3. Broker líder de la partición 0 antes de la caída.
4. Salida después de detener el broker.
5. Salida después de reiniciar el broker.
6. Conteo final de mensajes.
7. Respuestas:
   - ¿Qué archivos componen un segmento de Kafka?
   - ¿Qué pasó con el ISR durante la caída?
   - ¿Por qué no se perdieron mensajes?
   - ¿Qué riesgo habría con `min.insync.replicas=1` y `acks=1`?

## Limpieza

Para detener sin borrar datos:

```bash
docker compose -f docker-compose-lab-kafka.yml stop
```

Para borrar todo y empezar de cero, solo con autorización del docente:

```bash
docker compose -f docker-compose-lab-kafka.yml down -v
```

