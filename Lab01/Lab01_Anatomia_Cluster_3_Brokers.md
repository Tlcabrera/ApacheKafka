# Laboratorio 01 — Anatomía de un clúster de tres brokers
**Curso:** Apache Kafka para la gestión de flujos de datos en tiempo real · DIAN
**Sesión 2** · Módulo 2.1 · Duración: 35 min guiados en vivo + informe fuera de sesión
**Modalidad actualizada:** guiado en vivo sobre la instancia AWS EC2 del curso. Los participantes ingresan por navegador a code-server; no instalan Docker, Java ni Python en equipos corporativos.

## Objetivo
Levantar el entorno del curso, crear un topic replicado, observar los archivos del log en disco y provocar la caída del broker líder para ver cómo cambian el líder y el ISR de una partición.

## Resultados de aprendizaje que evidencia
RAE 1 — Explicar la arquitectura de Kafka (brokers, réplicas, ISR) y relacionarla con la durabilidad.

## Prerrequisitos
- Ingreso al code-server asignado por equipo.
- Repositorio del curso disponible en `~/kafka-dian`.
- El clúster se ejecuta en una instancia EC2 compartida; las acciones `docker stop`, `docker start`, `docker compose down` y limpieza de volúmenes deben hacerse solo cuando el docente lo indique.
- Para evitar colisiones, cada equipo debe usar prefijo en sus topics: `equipoXX.lab01...`.

> **Convención de la guía.** Los comandos se ejecutan desde una terminal integrada de code-server. Primero ubíquese en `~/kafka-dian/entorno`. Las herramientas de Kafka se invocan **dentro del contenedor** con `docker exec`, por lo que el servidor de arranque es el listener interno `broker1:19092`. Para abreviar, defina una vez:
> ```bash
> KB=broker1
> kt() { docker exec "$KB" /opt/kafka/bin/kafka-topics.sh --bootstrap-server "$KB":19092 "$@"; }
> ```
> En el navegador, Kafka UI se abre en `http://35.153.139.120:8080`.

---

## Paso 1 — Ubicarse en el laboratorio
```bash
cd ~/kafka-dian/entorno
ls
```
**Salida esperada:** `docker-compose-lab-kafka.yml` y archivos `docker-compose.etapa*.yml`.

## Paso 2 — Levantar el clúster
```bash
docker compose -f docker-compose-lab-kafka.yml up -d
docker compose -f docker-compose-lab-kafka.yml ps
```
**Salida esperada:** `broker1`, `broker2`, `broker3` en estado `healthy` (puede tardar 30–60 s), `kafka-ui` en `running`, `bootstrap-topics` en `exited (0)`.

Abra la interfaz de administración en **http://35.153.139.120:8080** y verifique que el clúster `dian-lab` muestra 3 brokers en línea.

> Con Podman, el mensaje `Executing external compose provider "podman-compose"`
> es informativo: indica quién ejecutará Compose. No es un error por sí solo; lea
> las líneas que le siguen.

**Si falla:** `docker compose -f docker-compose-lab-kafka.yml logs broker1 | tail -50`. Los errores más comunes son servicios no inicializados, nombres de contenedor residuales o memoria insuficiente (ver Anexo de resolución de problemas).

## Paso 3 — Verificar el clúster desde la línea de comandos ★ (va al informe)
```bash
docker exec broker1 /opt/kafka/bin/kafka-broker-api-versions.sh --bootstrap-server broker1:19092 | grep -E "^broker[0-9]"
```
**Salida esperada:** tres líneas, una por broker, con el formato `broker1:19092 (id: 1 rack: null) -> (`.

Copie la salida al informe.

## Paso 4 — Crear un topic replicado
El script de arranque ya creó los cuatro topics del dominio del curso. Para este laboratorio cree uno propio:
```bash
EQUIPO=equipo01
TOPIC=$EQUIPO.lab01.facturacion.emitida
kt --create --topic "$TOPIC" --partitions 3 --replication-factor 3 --config min.insync.replicas=2
kt --list
```
**Salida esperada:** `Created topic equipo01.lab01.facturacion.emitida.` y, en la lista, los topics `dian.*` más el nuevo. Cambie `equipo01` por el equipo real.

## Paso 5 — Describir el topic: líder e ISR ★ (va al informe)
```bash
kt --describe --topic "$TOPIC"
```
**Salida esperada (los números pueden variar):**
```
Topic: lab01.facturacion.emitida  PartitionCount: 3  ReplicationFactor: 3  Configs: min.insync.replicas=2
  Topic: lab01.facturacion.emitida  Partition: 0  Leader: 2  Replicas: 2,3,1  Isr: 2,3,1
  Topic: lab01.facturacion.emitida  Partition: 1  Leader: 3  Replicas: 3,1,2  Isr: 3,1,2
  Topic: lab01.facturacion.emitida  Partition: 2  Leader: 1  Replicas: 1,2,3  Isr: 1,2,3
```
Registre en el informe: **¿qué broker es líder de la partición 0?** Ese es el broker que detendrá en el paso 7.

## Paso 6 — Producir eventos e inspeccionar el disco
Produzca 100 eventos sintéticos de facturación con clave = NIT:
```bash
python3 ../codigo/gen_eventos.py --topic "$TOPIC" --n 100
```
(Si no tiene Python con `confluent-kafka` instalado, use la alternativa por consola:)
```bash
for i in $(seq 1 100); do echo "900${i}|{\"nit\":\"900${i}\",\"valor\":$((RANDOM*10)),\"n\":$i}"; done | \
docker exec -i broker1 /opt/kafka/bin/kafka-console-producer.sh --bootstrap-server broker1:19092 \
  --topic "$TOPIC" --property parse.key=true --property key.separator="|"
```
Ahora mire lo que quedó en disco en el broker líder de la partición 0 (reemplace `brokerN` por el líder anotado en el paso 5):
```bash
docker exec brokerN ls -lh /var/lib/kafka/data/${TOPIC}-0/
```
**Salida esperada:**
```
00000000000000000000.index
00000000000000000000.log
00000000000000000000.timeindex
leader-epoch-checkpoint
partition.metadata
```
Vea el contenido real del segmento:
```bash
docker exec brokerN /opt/kafka/bin/kafka-dump-log.sh --print-data-log \
  --files /var/lib/kafka/data/${TOPIC}-0/00000000000000000000.log | head -20
```
Observe los campos `offset`, `CreateTime`, `key` y `payload`.

## Paso 7 — Provocar la caída del líder ★ (va al informe)
1. Detenga el broker líder de la partición 0:
   ```bash
   docker stop brokerN
   ```
2. Espere 10 segundos y describa de nuevo el topic. Si detuvo `broker1`, cambie el cliente del alias a un broker vivo antes de ejecutar `kt`:
   ```bash
   KB=broker2
   ```
   ```bash
   kt --describe --topic "$TOPIC"
   ```
   **Observe:** la partición 0 tiene un **nuevo líder** y su `Isr` tiene **dos** brokers en lugar de tres. En Kafka UI, el topic aparece con una partición "under-replicated".
3. Produzca 10 eventos más (repita el comando del paso 6 con `--n 10`). ¿Acepta la escritura? Debe aceptarla: hay 2 réplicas en el ISR y `min.insync.replicas=2`.
4. Reinicie el broker y vuelva a describir:
   ```bash
   docker start brokerN
   sleep 20
   kt --describe --topic "$TOPIC"
   ```
   **Observe:** el ISR vuelve a tener tres brokers. El liderazgo puede o no regresar al broker original (la elección preferida de líder es periódica).
5. Consuma desde el inicio y cuente los mensajes:
   ```bash
   docker exec broker1 /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server broker1:19092 \
     --topic "$TOPIC" --from-beginning --timeout-ms 10000 2>/dev/null | wc -l
   ```
   **Salida esperada:** `110` (100 + 10). No se perdió ningún evento.

## Paso 8 — Limpieza (opcional; el entorno se reutiliza en la sesión 4)
```bash
docker compose -f docker-compose-lab-kafka.yml stop        # conserva los datos
# docker compose -f docker-compose-lab-kafka.yml down -v   # elimina todo; solo docente
```

---

## Informe de laboratorio (entregable en el portafolio, fecha límite: martes 29/09)
Documento de 1 a 2 páginas con:
1. Salida del paso 3 (brokers del clúster).
2. Salida del paso 5 (descripción del topic) con el líder de la partición 0 identificado.
3. Salidas del paso 7 antes y después de reiniciar el broker.
4. Respuesta a las tres preguntas:
   - **P1.** ¿Cuántos archivos componen un segmento y para qué sirve cada uno?
   - **P2.** ¿Qué pasó con el ISR mientras el broker estuvo caído y por qué el clúster siguió aceptando escrituras?
   - **P3.** ¿Se perdieron mensajes? Explique qué configuración lo garantizó y qué habría pasado con `min.insync.replicas=1` y `acks=1` si el líder hubiera caído justo después de confirmar una escritura.
5. Un párrafo relacionando el ejercicio con un flujo real de su dependencia.

Se califica con la **rúbrica genérica de laboratorio** (ver carpeta Rúbricas).

## Anexo — Resolución de problemas
| Síntoma | Causa probable | Solución |
|---|---|---|
| `port is already allocated` | Otro servicio usa 9092/8080 | Cambiar el puerto en `docker-compose.yml` o detener el servicio que lo ocupa |
| `container name "broker1" is already in use` o error de pod de Podman | Un intento anterior dejó el pod o los contenedores del mismo laboratorio | Primero inspeccione: `podman ps -a --format '{{.Names}}\t{{.Status}}'` y `podman pod ps --format '{{.Name}}\t{{.Status}}'`. Si confirma que el pod corresponde **solo** a este laboratorio, elimínelo con `podman pod rm -f <nombre-del-pod>` y ejecute de nuevo el paso 2. Si no hay pod, elimine únicamente los contenedores que confirmó como residuales: `podman rm -f broker1 broker2 broker3 kafka-ui bootstrap-topics`. No use estos comandos sobre contenedores de otra práctica. |
| Los brokers reinician en bucle | Memoria insuficiente | Asignar 10 GB al motor de contenedores (Rancher Desktop: Preferences → Virtual Machine; Podman: `podman machine set --memory 10240`) y usar un host de 16 GB o más |
| `bootstrap-topics` en `exited (1)` | Los brokers no estaban listos | `docker compose up bootstrap-topics` de nuevo |
| `Connection to node -1 could not be established` | Se usó `localhost:19092` desde fuera del contenedor | Desde el host use `localhost:9092`; desde `docker exec` use `broker1:19092` |
| `kt: command not found` en PowerShell | El alias es de bash | Use la función de PowerShell indicada arriba o el comando completo |
