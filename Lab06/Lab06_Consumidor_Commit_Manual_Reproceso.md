# Laboratorio 06 — Consumidor con commit manual, reprocesamiento y DLQ
**Sesión 8** · Módulo 4.2 · 35 min guiados en vivo · Entregable: código + informe (martes 20/10)

## Objetivo
Implementar un consumidor con commit manual que persista en PostgreSQL, interrumpirlo entre el `INSERT` y el commit para observar el reproceso, convertirlo en idempotente con `UPSERT`, reposicionar el grupo a una marca de tiempo e implementar el patrón de cola de mensajes fallidos con tres reintentos.

## Prerrequisitos
- En AWS/code-server, abra terminal y ubíquese en el repositorio:
  ```bash
  cd ~/kafka-dian
  EQUIPO=equipo01
  TOPIC=$EQUIPO.lab05.facturacion.emitida
  GROUP=${EQUIPO}-lab06-sink
  ```
- Levante la etapa con PostgreSQL desde `~/kafka-dian/entorno`:
  ```bash
  cd ~/kafka-dian/entorno
  docker compose -f docker-compose-lab-kafka.yml -f docker-compose.override.yml up -d
  cd ~/kafka-dian
  ```
- Dependencias Python instaladas en la instancia: `confluent-kafka` y `psycopg2-binary`.
- Topic con datos: `python3 codigo/gen_eventos.py --topic "$TOPIC" --n 500`.

## Paso 1 — Consumidor con commit manual ★
Revise `consumidor_facturacion.py`: `enable.auto.commit=false` y `c.commit(message=msg)` **después** de `pg.commit()`. Ejecute y deje que procese todo:
```bash
python3 codigo/consumidor_facturacion.py --topic "$TOPIC" --group "$GROUP"
# Ctrl+C al ver que deja de imprimir
docker exec -it postgres psql -U dian -c "SELECT count(*) FROM facturas;"
```
**Salida esperada:** 500.

## Paso 2 — Caída entre el INSERT y el commit ★
Reinicie la tabla y el grupo, y simule la caída tras 60 mensajes (el esqueleto sale sin confirmar el offset del último mensaje):
```bash
docker exec -it postgres psql -U dian -c "TRUNCATE facturas;"
docker exec broker1 /opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server broker1:19092 --group "$GROUP" --topic "$TOPIC" --reset-offsets --to-earliest --execute
python3 codigo/consumidor_facturacion.py --topic "$TOPIC" --group "$GROUP" --kill-after 60
python3 codigo/consumidor_facturacion.py --topic "$TOPIC" --group "$GROUP"        # reinicio normal; Ctrl+C al terminar
docker exec -it postgres psql -U dian -c "SELECT count(*) total, count(DISTINCT id_evento) unicos FROM facturas;"
```
**Salida esperada:** el segundo arranque **falla** en el primer mensaje con `duplicate key value violates unique constraint` (o, si quita la clave primaria, verá `total > unicos`). Es el reproceso de *at least once*: el mensaje 60 se escribió pero su offset no se confirmó.

## Paso 3 — Idempotencia con UPSERT ★
Repita el paso 2 con `--upsert`:
```bash
docker exec -it postgres psql -U dian -c "TRUNCATE facturas;"
docker exec broker1 /opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server broker1:19092 --group "$GROUP" --topic "$TOPIC" --reset-offsets --to-earliest --execute
python3 codigo/consumidor_facturacion.py --topic "$TOPIC" --group "$GROUP" --upsert --kill-after 60
python3 codigo/consumidor_facturacion.py --topic "$TOPIC" --group "$GROUP" --upsert
docker exec -it postgres psql -U dian -c "SELECT count(*) total, count(DISTINCT id_evento) unicos FROM facturas;"
```
**Salida esperada:** `500 | 500`. El reproceso fue inocuo.

## Paso 4 — Reposicionar el grupo a una marca de tiempo ★
Con los consumidores detenidos:
```bash
FECHA=$(date -u -d '10 minutes ago' +%Y-%m-%dT%H:%M:%S.000)   # macOS: date -u -v-10M +%Y-%m-%dT%H:%M:%S.000
docker exec broker1 /opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server broker1:19092 --group "$GROUP" \
  --topic "$TOPIC" --reset-offsets --to-datetime $FECHA --dry-run
# revisar la columna NEW-OFFSET y luego:
docker exec broker1 /opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server broker1:19092 --group "$GROUP" \
  --topic "$TOPIC" --reset-offsets --to-datetime $FECHA --execute
python3 codigo/consumidor_facturacion.py --topic "$TOPIC" --group "$GROUP" --upsert     # reprocesa desde esa hora sin duplicar
```
Registre los offsets antes y después.

## Paso 5 — Mensaje malformado ★
```bash
echo '800999|{esto no es json' | docker exec -i broker1 /opt/kafka/bin/kafka-console-producer.sh --bootstrap-server broker1:19092 \
  --topic "$TOPIC" --property parse.key=true --property key.separator="|"
python3 codigo/consumidor_facturacion.py --topic "$TOPIC" --group "$GROUP" --upsert
```
**Observe:** tres intentos con `JSONDecodeError`, luego `-> enviado a DLQ`, y el consumidor **continúa** con los siguientes mensajes. Sin el patrón, la partición quedaría bloqueada.

## Paso 6 — Inspeccionar la DLQ ★
```bash
docker exec broker1 /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server broker1:19092 --topic "${TOPIC}.dlq" \
  --from-beginning --timeout-ms 5000 --property print.headers=true --property print.key=true
```
Verifique los encabezados `origen.topic`, `origen.particion`, `origen.offset`, `error`, `consumidor`, `ts`. Proponga en el informe **un encabezado adicional** que la DIAN necesitaría para atender el mensaje (p. ej., dependencia responsable).

## Informe
Salidas de los pasos ★, y: **(a)** explique con sus palabras por qué el paso 2 falló y el 3 no; **(b)** ¿en qué caso preferiría "guardar el offset en el destino" en lugar del UPSERT?; **(c)** quién debería ser el responsable de la DLQ del flujo de facturación en la entidad y qué acuerdo de atención propondría.
