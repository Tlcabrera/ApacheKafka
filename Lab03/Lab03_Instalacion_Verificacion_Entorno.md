# Laboratorio 03 — Instalación y verificación del entorno
**Sesión 5** · Módulo 3.1 · 50 min guiados en vivo · Entregable: lista de verificación diligenciada con capturas (jueves 08/10)
**Criterio de cierre:** ningún participante avanza a la sesión 6 sin la lista aprobada.

## Objetivo
Instalar manualmente un nodo único de Kafka en modo KRaft para comprender el proceso, y dejar operativo el entorno oficial del curso en contenedores con los cuatro topics del dominio sintético.

## Prerrequisitos
- En el entorno AWS del curso, el estudiante no instala herramientas en el equipo corporativo: ingresa por navegador a su code-server y trabaja dentro de la EC2 preparada.
- JDK 17, Git, Python 3 y el motor de contenedores ya deben estar disponibles en la instancia.
- La parte de instalación manual puede ejecutarse como demostración del docente o en turnos; no debe dejar un proceso Kafka manual ocupando puertos antes de levantar el entorno oficial.

> **Nota de licenciamiento (leer antes de instalar).** No use **Docker Desktop**: su licencia exige suscripción de pago para entidades de gobierno, y las estaciones de este curso son equipos institucionales. Las tres alternativas indicadas son libres (Apache 2.0 / MIT), ejecutan todos los laboratorios sin cambiar un solo comando y, con Podman, basta con definir `alias docker=podman` y `alias docker-compose=podman-compose`. Detalle completo en `Licenciamiento/Anexo_Licenciamiento_Herramientas.md`.
- Distribución descargada desde https://kafka.apache.org/downloads (binario `kafka_2.13-3.9.x.tgz`), tarea de la sesión 4.
- Windows: usar WSL2 (Ubuntu) para la parte 1; los `.bat` de `bin/windows/` funcionan pero la guía asume shell POSIX.

---

## Parte 1 — Instalación manual de un nodo (20 min)

### Paso 1 — Descomprimir y revisar la estructura
```bash
tar -xzf kafka_2.13-3.9.*.tgz && cd kafka_2.13-3.9.*/
ls bin | head; ls config/kraft
```
**Observe:** `bin/` (herramientas), `config/kraft/server.properties` (nodo combinado), `libs/`. No hay instalador ni servicio: Kafka es un directorio.

### Paso 2 — Generar el identificador del clúster y formatear ★
```bash
export KAFKA_CLUSTER_ID=$(bin/kafka-storage.sh random-uuid)
echo $KAFKA_CLUSTER_ID
bin/kafka-storage.sh format -t $KAFKA_CLUSTER_ID -c config/kraft/server.properties
```
**Salida esperada:** `Formatting metadata directory /tmp/kraft-combined-logs with metadata.version 3.9-IV0.`
Anote el `cluster.id`. **Pregunta:** ¿qué pasaría si dos brokers se formatearan con identificadores distintos y se intentaran unir al mismo clúster?

### Paso 3 — Arrancar y leer el log ★
```bash
bin/kafka-server-start.sh config/kraft/server.properties > /tmp/kafka-manual.log 2>&1 &
sleep 8
grep -E "Kafka Server started|KafkaRaftServer|Transition from|ERROR" /tmp/kafka-manual.log | head
```
**Salida esperada:** una línea con `[KafkaRaftServer nodeId=1] Kafka Server started` y transiciones del controlador (`Transition from … to Leader`). Ninguna línea `ERROR`.
Verifique los puertos:
```bash
ss -ltnp | grep -E "9092|9093"     # o: netstat -an | grep -E "9092|9093"
```

### Paso 4 — Producir y consumir un mensaje de prueba ★
```bash
bin/kafka-topics.sh --create --topic prueba.instalacion --bootstrap-server localhost:9092
echo "hola desde la instalacion manual" | bin/kafka-console-producer.sh --bootstrap-server localhost:9092 --topic prueba.instalacion
bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic prueba.instalacion --from-beginning --timeout-ms 5000
bin/kafka-metadata-quorum.sh --bootstrap-server localhost:9092 describe --status | head -3
```
**Salida esperada:** el mensaje se imprime; el quórum muestra `LeaderId: 1`.

### Paso 5 — Detener el nodo manual
```bash
bin/kafka-server-stop.sh; sleep 3; ss -ltnp | grep 9092 || echo "puerto 9092 libre"
```
Debe quedar libre: el entorno en contenedores usa el mismo puerto.

## Parte 2 — Entorno oficial del curso (25 min)

### Paso 6 — Levantar el entorno oficial en AWS/code-server
```bash
cd ~/kafka-dian/entorno
docker compose -f docker-compose-lab-kafka.yml up -d
sleep 45
docker compose -f docker-compose-lab-kafka.yml ps
```
Los tres brokers en `healthy`; `bootstrap-topics` en `exited (0)`.

### Paso 7 — Verificar la interfaz y los topics ★
Abra http://35.153.139.120:8080 → clúster `dian-lab` → Topics. Deben existir:
`dian.facturacion.emitida` (6 part.) · `dian.declaracion.presentada` (6) · `dian.aduanas.declaracion-importacion` (3) · `dian.auditoria.acceso` (6, compact).
Por consola:
```bash
docker exec broker1 /opt/kafka/bin/kafka-topics.sh --bootstrap-server broker1:19092 --describe --topic dian.auditoria.acceso | head -2
```
**Observe** `Configs: cleanup.policy=compact,min.insync.replicas=2`.

### Paso 8 — Generar eventos y verificar la llegada ★
```bash
cd ~/kafka-dian
python3 codigo/gen_eventos.py --topic dian.facturacion.emitida --rate 50 --minutes 2
```
En Kafka UI → topic → Messages, observe los eventos con clave NIT. Por consola, cuente:
```bash
docker exec broker1 /opt/kafka/bin/kafka-run-class.sh kafka.tools.GetOffsetShell --broker-list broker1:19092 --topic dian.facturacion.emitida
```
La suma de los offsets de las 6 particiones debe aproximarse a 6.000.

### Paso 9 — Lista de verificación (entregable)
| Ítem | Valor / captura |
|---|---|
| Sistema operativo y versión | |
| Distribución y versión de JDK (`java -version`) | |
| Motor de contenedores elegido y versión (`docker --version` o `podman --version`) | |
| `python3 --version` y `python3 -m pip show confluent-kafka` (versión) | |
| Versión de Kafka instalada manualmente y `cluster.id` (paso 2) | |
| Captura del paso 3 (línea "Kafka Server started") | |
| Captura del paso 4 (mensaje consumido) | |
| Captura de Kafka UI con los 4 topics (paso 7) | |
| Conteo de mensajes del paso 8 | |
| RAM del equipo y RAM asignada a Docker | |
| Firma y fecha | |

## Anexo — Errores frecuentes
| Síntoma | Solución |
|---|---|
| `Address already in use` en el paso 3 | Otro proceso en 9092: `ss -ltnp \| grep 9092` y detenerlo; o el nodo manual sigue vivo tras la parte 1 |
| `The Cluster ID … doesn't match stored clusterId` | Se formateó dos veces con ids distintos: `rm -rf /tmp/kraft-combined-logs` y repetir el paso 2 |
| `java: command not found` | Instalar JDK 17 y exportar `JAVA_HOME` |
| `ModuleNotFoundError: confluent_kafka` | `pip install confluent-kafka` en el mismo intérprete que ejecuta el script |
| Brokers no `healthy` en AWS | Avisar al docente. Verificar desde `~/kafka-dian/entorno`: `docker compose -f docker-compose-lab-kafka.yml logs broker1 --tail 80` |
| WSL2: `localhost:9092` no responde desde Windows | Ejecutar los scripts Python dentro de WSL, o usar la IP de WSL |
