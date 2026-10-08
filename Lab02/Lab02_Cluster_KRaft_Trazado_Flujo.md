# Laboratorio 02 — Clúster en modo KRaft y trazado del flujo de un evento
**Curso:** Apache Kafka para la gestión de flujos de datos en tiempo real · DIAN
**Sesión 4** · Módulo 2.3 · Duración: 35 min guiados en vivo + informe y diagrama fuera de sesión

## Objetivo
Inspeccionar el quórum de controladores KRaft, provocar la caída del controlador activo y medir el tiempo de elección; producir y consumir un mensaje registrando sus offsets; y elaborar un diagrama propio del recorrido del mensaje anotando el parámetro que gobierna cada paso.

## Resultados de aprendizaje que evidencia
RAE 1 — Explicar el rol del controlador y trazar el ciclo de vida de un evento.

## Prerrequisitos
- Entorno del Laboratorio 01 en ejecución en la instancia EC2 compartida. Los tres nodos corren en modo KRaft con roles combinados `broker,controller`.
- Ingrese a code-server, abra terminal y ejecute `cd ~/kafka-dian/entorno`.
- Si el entorno está detenido, el docente o el equipo asignado lo inicia con `docker compose -f docker-compose-lab-kafka.yml up -d`.
- Cada equipo usa un topic propio: `equipoXX.lab02.declaracion.presentada`.
- Alias sugeridos (bash):
  ```bash
  alias kq='docker exec broker1 /opt/kafka/bin/kafka-metadata-quorum.sh --bootstrap-server broker1:19092'
  alias kt='docker exec broker1 /opt/kafka/bin/kafka-topics.sh --bootstrap-server broker1:19092'
  ```

---

## Paso 1 — Verificar el entorno
```bash
docker compose -f docker-compose-lab-kafka.yml ps
```
Los tres brokers deben estar `healthy`.

## Paso 2 — Describir el quórum de controladores ★ (va al informe)
```bash
kq describe --status
```
**Salida esperada (valores ilustrativos):**
```
ClusterId:              dian-kafka-lab-2026
LeaderId:               2
LeaderEpoch:            5
HighWatermark:          1480
MaxFollowerLag:         0
MaxFollowerLagTimeMs:   0
CurrentVoters:          [1,2,3]
CurrentObservers:       []
```
Registre **LeaderId** (el controlador activo) y **LeaderEpoch**. Vea también la replicación del log de metadatos:
```bash
kq describe --replication
```
Cada votante muestra su `LogEndOffset` y su `Lag` respecto al líder; el lag debe ser 0 o muy pequeño.

## Paso 3 — Mirar el log de metadatos
Los metadatos viven en una partición interna, `__cluster_metadata-0`, con el mismo formato de segmentos que cualquier topic:
```bash
docker exec broker1 ls -lh /var/lib/kafka/data/__cluster_metadata-0/
```
Decodifique sus registros:
```bash
docker exec broker1 /opt/kafka/bin/kafka-dump-log.sh --cluster-metadata-decoder \
  --files /var/lib/kafka/data/__cluster_metadata-0/00000000000000000000.log | grep -E "TOPIC_RECORD|PARTITION_RECORD|REGISTER_BROKER" | head -20
```
Identifique al menos un `REGISTER_BROKER_RECORD` (registro de un broker) y un `TOPIC_RECORD` (creación de un topic).

## Paso 4 — Crear un topic y encontrar su registro ★ (va al informe)
```bash
EQUIPO=equipo01
TOPIC=$EQUIPO.lab02.declaracion.presentada
kt --create --topic "$TOPIC" --partitions 3 --replication-factor 3
docker exec broker1 /opt/kafka/bin/kafka-dump-log.sh --cluster-metadata-decoder \
  --files /var/lib/kafka/data/__cluster_metadata-0/00000000000000000000.log | grep -A3 "$TOPIC" | head -20
```
**Observe:** un `TOPIC_RECORD` con el nombre y el `topicId`, seguido de tres `PARTITION_RECORD` con `replicas`, `isr` y `leader`. **Esa es la "verdad" del clúster:** el líder de cada partición no está en ZooKeeper ni en memoria de un nodo, está en este log replicado.

> Si el segmento rotó, liste los archivos del paso 3 y use el `.log` más reciente.

## Paso 5 — Caída y elección del controlador ★ (va al informe)
1. Anote el `LeaderId` del paso 2 (por ejemplo, 2).
2. Prepare un cronómetro (o use `date +%T` antes y después).
3. Detenga ese nodo y consulte el quórum desde **otro** broker. Reemplace los
   valores por el `LeaderId` real y un broker que siga vivo:
   ```bash
   CONTROLADOR=2       # cambie por el LeaderId anotado en el paso 2
   BROKER_VIVO=1       # use 2 o 3 si CONTROLADOR es 1
   date +%T
   docker stop broker$CONTROLADOR
   docker exec broker$BROKER_VIVO /opt/kafka/bin/kafka-metadata-quorum.sh \
     --bootstrap-server broker$BROKER_VIVO:19092 describe --status
   date +%T
   ```
   Si el comando falla la primera vez (aún no hay líder), repítalo tras 2 segundos.
4. **Registre:** el nuevo `LeaderId`, el nuevo `LeaderEpoch` (debe haber aumentado) y el tiempo aproximado transcurrido. Típicamente 1–3 segundos.
5. Describa el topic del paso 4 desde el broker que sigue vivo: las particiones
   cuyo líder estaba en el nodo detenido tienen nuevo líder y un ISR de dos.
   ```bash
   docker exec broker$BROKER_VIVO /opt/kafka/bin/kafka-topics.sh \
     --bootstrap-server broker$BROKER_VIVO:19092 --describe \
     --topic "$TOPIC"
   ```
6. Reinicie el nodo: `docker start broker$CONTROLADOR` y espere 20 s. Vuelva a describir el quórum: el nodo reaparece en `CurrentVoters` con lag 0 tras sincronizar.

**Pregunta para el informe:** ¿qué habría pasado si hubiera detenido **dos** de los tres nodos? (Pista: mayoría.)

## Paso 6 — Producir y consumir registrando offsets ★ (va al informe)
Produzca tres mensajes con clave:
```bash
printf "830001|{\"nit\":\"830001\",\"formulario\":\"110\",\"periodo\":\"2026-08\"}\n830001|{\"nit\":\"830001\",\"formulario\":\"300\",\"periodo\":\"2026-08\"}\n900123|{\"nit\":\"900123\",\"formulario\":\"110\",\"periodo\":\"2026-08\"}\n" | \
docker exec -i broker1 /opt/kafka/bin/kafka-console-producer.sh --bootstrap-server broker1:19092 \
  --topic "$TOPIC" --property parse.key=true --property key.separator="|"
```
Consuma desde el inicio mostrando partición y offset:
```bash
docker exec broker1 /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server broker1:19092 \
  --topic "$TOPIC" --from-beginning --timeout-ms 8000 \
  --property print.key=true --property print.partition=true --property print.offset=true 2>/dev/null
```
**Salida esperada (particiones ilustrativas):**
```
Partition:1  Offset:0  830001  {"nit":"830001","formulario":"110",...}
Partition:1  Offset:1  830001  {"nit":"830001","formulario":"300",...}
Partition:0  Offset:0  900123  {"nit":"900123","formulario":"110",...}
```
**Observe:** los dos mensajes del NIT 830001 cayeron en la **misma partición y en orden** (offsets 0 y 1); el del NIT 900123 fue a otra partición con su propio offset 0.

Ahora consuma con un **grupo** y vea dónde queda registrado su avance:
```bash
docker exec broker1 /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server broker1:19092 \
  --topic "$TOPIC" --from-beginning --group "${EQUIPO}-lab02-lector" --timeout-ms 8000 2>/dev/null >/dev/null
docker exec broker1 /opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server broker1:19092 --describe --group "${EQUIPO}-lab02-lector"
```
**Salida esperada:** una fila por partición con `CURRENT-OFFSET`, `LOG-END-OFFSET` y `LAG` = 0. Esa posición está guardada en el topic interno `__consumer_offsets`:
```bash
kt --list | grep consumer_offsets
```

## Paso 7 — Diagrama propio del flujo ★ (va al informe)
Dibuje (a mano, en diagrams.net, PowerPoint o cualquier herramienta) el recorrido de **uno** de los mensajes del paso 6 desde el productor hasta el registro del offset del grupo, en al menos siete pasos. En cada paso anote **el parámetro de configuración que lo gobierna**. Como guía mínima:

| Paso | Qué ocurre | Parámetro(s) a anotar |
|---|---|---|
| 1 | El productor obtiene metadatos y descubre el líder | `bootstrap.servers`, `metadata.max.age.ms` |
| 2 | Serializa y calcula la partición por la clave | `key.serializer`, `partitioner.class` |
| 3 | Acumula en lote y envía | `batch.size`, `linger.ms`, `compression.type` |
| 4 | El líder anexa al segmento activo | `log.segment.bytes` |
| 5 | Las réplicas traen los datos; avanza el high water mark | `replication.factor`, `min.insync.replicas`, `replica.lag.time.max.ms` |
| 6 | El líder confirma al productor | `acks` |
| 7 | El consumidor lee desde su offset y confirma | `group.id`, `auto.offset.reset`, `enable.auto.commit` |

---

## Informe de laboratorio (entregable en el portafolio, fecha límite: martes 06/10)
1. Salida del paso 2 con `LeaderId` y `LeaderEpoch`.
2. Registros del paso 4 (`TOPIC_RECORD` y un `PARTITION_RECORD`).
3. Salidas del paso 5 antes y después de la caída, con el tiempo de elección medido, y la respuesta a la pregunta de los dos nodos.
4. Salida del paso 6 con particiones y offsets, y la del grupo `${EQUIPO}-lab02-lector`.
5. El diagrama del paso 7.
6. Un párrafo: ¿por qué KRaft simplifica la operación frente a ZooKeeper en una entidad como la DIAN?

Se califica con la **rúbrica genérica de laboratorio**; el diagrama pesa el doble en el criterio "Corrección técnica".
