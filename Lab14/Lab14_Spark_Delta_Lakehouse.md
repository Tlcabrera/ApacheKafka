# Laboratorio 14 — De Kafka a Delta con Spark Structured Streaming

**Sesiones 18 y 19** · Módulo 8.1 · 80 min guiados · Informe/proyecto: jueves 26/11

## Qué replica este laboratorio

Replica una versión didáctica de un *lakehouse*: Kafka recibe eventos de
facturación sintéticos; Spark conserva la evidencia de llegada en **bronce** y
publica una vista analítica **plata** validada y deduplicada en Delta Lake sobre
MinIO (S3 local). No representa una plataforma productiva ni usa datos de
contribuyentes. El valor del experimento es observar la relación entre offsets
de Spark, checkpoints y reintentos.

## Objetivo

Consumir `dian.facturacion.emitida` desde Spark, escribir la capa bronce en
Delta particionada por fecha, construir plata deduplicada con una tabla de
rechazos y verificar qué ocurre al perder solamente el checkpoint de bronce.

## Extension Databricks (sesion 19)

La segunda sesion del laboratorio conecta el ejercicio local con una arquitectura
Databricks. El objetivo no es depender de una cuenta Databricks para aprobar el
curso, sino que el participante entienda como trasladar el patron Kafka ->
Structured Streaming -> Delta hacia un workspace administrado.

Temas minimos:

- Equivalencia entre MinIO/S3 local y almacenamiento cloud usado por Databricks.
- Checkpoints de Structured Streaming y continuidad operacional.
- Delta Lake como formato de tabla y frontera entre bronce, plata y rechazos.
- Auto Loader y Delta Live Tables como alternativas administradas, si la
  entidad las tiene disponibles.
- Unity Catalog como referencia para gobierno, permisos y linaje.
- Criterios para ubicar logica en Kafka Streams, Spark local o Databricks.

Si existe un workspace Databricks autorizado, la sesion 19 puede incluir una
demo corta de lectura/escritura Delta. Si no existe, se trabaja con el mismo
laboratorio Spark/Delta local y un diagrama de arquitectura comparativa.

## Antes de empezar

Esta etapa usa los tres brokers más Schema Registry, Connect, PostgreSQL,
MinIO y un proceso Spark. En el curso actualizado se ejecuta sobre la instancia
EC2 compartida, no sobre los equipos corporativos. Para 20 estudiantes, evite
que todos ejecuten simultáneamente el trabajo Spark: el docente debe organizar
turnos, una demo central o grupos reducidos según la capacidad real de la
instancia.

Desde la raíz del repositorio, inicie las etapas A/B, C y F:

```bash
cd ~/kafka-dian/entorno
docker compose \
  -f docker-compose-lab-kafka.yml \
  -f docker-compose.etapaC.yml \
  -f docker-compose.etapaF.yml up -d
docker compose \
  -f docker-compose-lab-kafka.yml \
  -f docker-compose.etapaC.yml \
  -f docker-compose.etapaF.yml ps -a minio-init spark
cd ..
```

`minio-init` debe terminar con código `0`: crea, sin borrar datos previos, los
buckets `dian-bronce`, `dian-plata` y `dian-checkpoints`. El servicio `spark`
debe quedar en ejecución. La primera ejecución de Spark descargará conectores
Maven fijados en `entorno/spark-defaults.conf` y los conserva en el volumen
`spark-ivy`; por tanto necesita acceso al repositorio de dependencias una sola
vez.

En otra terminal, genere eventos sintéticos de forma continua. Active primero
el entorno virtual del curso si existe:

```bash
cd ~/kafka-dian
source .venv-kafka/bin/activate
python3 codigo/gen_eventos.py \
  --topic dian.facturacion.emitida --rate 100 --minutes 20
```

Si `confluent_kafka` no está instalado, ejecute una vez
`python3 -m pip install confluent-kafka` dentro de ese entorno virtual.

## Paso 1 — Lectura del topic ★

Abra una consola PySpark configurada con Delta y S3A:

```bash
docker exec -it spark /opt/spark/bin/pyspark
```

```python
df = (spark.readStream.format("kafka")
      .option("kafka.bootstrap.servers", "broker1:19092")
      .option("subscribe", "dian.facturacion.emitida")
      .option("startingOffsets", "earliest").load())
df.printSchema()
```

**Salida esperada:** el esquema fijo de Kafka (`key`, `value`, `topic`,
`partition`, `offset`, `timestamp`). El mensaje sigue siendo binario hasta que
se deserializa: esa es la primera lección.

Salga con `exit()` antes del siguiente paso.

## Paso 2 — Bronce, plata y rechazos en Delta ★

En una terminal diferente ejecute el trabajo y déjelo corriendo:

```bash
docker exec -it spark /opt/spark/bin/spark-submit \
  /opt/spark/work-dir/codigo/spark_kafka_a_delta.py
```

Después de dos minutos, abra otra consola `pyspark` como en el paso 1 y
consulte:

```python
spark.read.format("delta").load("s3a://dian-bronce/facturacion").count()
spark.read.format("delta").load("s3a://dian-bronce/facturacion") \
     .select("fecha").distinct().show()
```

**Observe** la partición `fecha` y las columnas `particion`, `offset` e
`ingerido_en`: bronce es la evidencia de lo que llegó, incluso si un evento es
inválido para analítica.

## Paso 3 — Dónde viven los offsets ★

Desde la raíz del repositorio, liste el checkpoint con el cliente MinIO
incluido para el laboratorio:

```bash
bash entorno/minio.sh ls -r local/dian-checkpoints/bronce/offsets
docker exec broker1 /opt/kafka/bin/kafka-consumer-groups.sh \
  --bootstrap-server broker1:19092 --list | grep -i spark \
  || echo "Spark no aparece como grupo de consumidores"
```

**Punto clave:** Spark no confirma sus posiciones en `__consumer_offsets` ni
aparece en `kafka-consumer-groups`. El progreso se consulta en el checkpoint y
en métricas del job, por ejemplo `spark.streams.active[0].lastProgress` dentro
de una sesión PySpark conectada al driver.

## Paso 4 — Plata: deduplicación y rechazos ★

Inyecte un evento inválido desde el host:

```bash
cd ~/kafka-dian
source .venv-kafka/bin/activate
python3 codigo/gen_eventos.py \
  --topic dian.facturacion.emitida --n 1 --valor-total -1
```

Tras unos segundos, desde PySpark:

```python
spark.read.format("delta").load("s3a://dian-plata/facturacion").count()
spark.read.format("delta").load("s3a://dian-plata/facturacion_rechazos") \
     .groupBy("motivo_rechazo").count().show()
```

El evento negativo debe aparecer en rechazos y no en plata. Plata aplica
`dropDuplicates(["id_evento"])` con estado propio y checkpoint propio.

## Paso 5 — Recuperación desde el checkpoint ★

1. En la terminal que ejecuta `spark-submit`, presione `Ctrl+C` y espere a que
   termine el proceso.
2. Produzca exactamente 500 eventos:

   ```bash
   cd ~/kafka-dian
   source .venv-kafka/bin/activate
   python3 codigo/gen_eventos.py --topic dian.facturacion.emitida --n 500
   ```

3. Ejecute de nuevo el mismo `spark-submit` del paso 2.

**Observe:** el conteo de bronce aumenta en 500 (además del evento inválido si
lo generó después de la medición inicial). El checkpoint de bronce conservaba
la posición Kafka; `startingOffsets=earliest` solo opera al crear un checkpoint
nuevo.

## Paso 6 — Borrar solo el checkpoint de bronce ★

Este es un experimento controlado: nunca elimine checkpoints de producción.
Detenga otra vez el trabajo y ejecute:

```bash
bash entorno/minio.sh rm -r --force local/dian-checkpoints/bronce
```

Vuelva a iniciar el trabajo. Tras darle tiempo para releer el topic, compare:

```python
spark.read.format("delta").load("s3a://dian-bronce/facturacion").count()
spark.read.format("delta").load("s3a://dian-plata/facturacion").count()
```

**Resultado esperado:** bronce aumenta porque su nuevo checkpoint empieza con
`earliest` y vuelve a dejar evidencia de los eventos. Plata no aumenta por esas
copias: el stream que lee Delta detecta nuevos commits de bronce, pero su estado
de deduplicación —que no se eliminó— ya conoce los `id_evento`. Si elimina
también `dian-checkpoints/plata`, pierde esa protección; ese caso queda fuera
del experimento.

## Informe

Incluya las salidas de los pasos ★, la comparación de conteos del paso 6, y
responda:

1. ¿Por qué `kafka-consumer-groups` no muestra el lag de Spark y cómo
   monitorearía el progreso del job?
2. ¿Qué política de respaldo y retención definiría para los checkpoints?
3. Para un flujo de su dependencia, ¿qué lógica ubicaría en Kafka Streams y
   cuál en el lakehouse/Databricks? Justifique el corte con el criterio de las
   sesiones 18 y 19.
