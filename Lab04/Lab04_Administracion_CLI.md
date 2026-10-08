# Laboratorio 04 — Administración por línea de comandos
**Sesión 6** · Módulo 3.2 · 25 min guiados en vivo · Informe breve en el portafolio (fecha límite: martes 13/10)

## Objetivo
Crear un topic con configuración específica, alterar dinámicamente su retención, aplicar una cuota de producción y comprobar su efecto, consultar la distribución de espacio y verificar el bloqueo de la autocreación de topics.

## Prerrequisitos
Entorno del curso en ejecución en AWS/code-server. Abra terminal y ejecute:
```bash
cd ~/kafka-dian/entorno
EQUIPO=equipo01
TOPIC=$EQUIPO.lab04.auditoria.acceso
```
Cambie `equipo01` por el equipo real. Alias:
```bash
alias kt='docker exec broker1 /opt/kafka/bin/kafka-topics.sh --bootstrap-server broker1:19092'
alias kc='docker exec broker1 /opt/kafka/bin/kafka-configs.sh --bootstrap-server broker1:19092'
```

## Paso 1 — Topic compactado con configuración propia ★
El script de arranque ya creó `dian.auditoria.acceso`; cree una versión de laboratorio:
```bash
kt --create --topic "$TOPIC" --partitions 6 --replication-factor 3 \
   --config cleanup.policy=compact --config min.insync.replicas=2 --config segment.bytes=1048576
kt --describe --topic "$TOPIC" | head -1
```
**Observe** la línea `Configs:` con los tres valores. `segment.bytes` pequeño (1 MB) permitirá ver la compactación actuar en pocos minutos.

## Paso 2 — Alterar dinámicamente la retención ★
```bash
kc --alter --entity-type topics --entity-name "$TOPIC" --add-config delete.retention.ms=60000,min.compaction.lag.ms=10000
kc --describe --entity-type topics --entity-name "$TOPIC"
```
**Observe:** los valores aparecen como `DYNAMIC_TOPIC_CONFIG`; no hubo reinicio. Elimine uno y verifique:
```bash
kc --alter --entity-type topics --entity-name "$TOPIC" --delete-config min.compaction.lag.ms
```

## Paso 3 — Aplicar una cuota de producción ★
Cuota de 1 MB/s para el `client.id` del generador:
```bash
kc --alter --entity-type clients --entity-name gen-facturacion --add-config producer_byte_rate=1048576
kc --describe --entity-type clients --entity-name gen-facturacion
```

## Paso 4 — Observar el efecto de la cuota ★
Produzca a alta tasa con y sin cuota y compare el rendimiento reportado:
```bash
cd ~/kafka-dian
python3 codigo/gen_eventos.py --topic dian.facturacion.emitida --n 20000            # client.id = gen-facturacion (con cuota)
python3 codigo/gen_eventos.py --topic dian.facturacion.emitida --n 20000 --client-id sin-cuota
```
Mida con `time` (o compare la duración impresa). Con eventos de ~350 bytes, 1 MB/s ≈ 3.000 ev/s: el primer comando debe tardar notablemente más. En Kafka UI → Brokers → Metrics puede verse el estrangulamiento.
Retire la cuota al terminar:
```bash
kc --alter --entity-type clients --entity-name gen-facturacion --delete-config producer_byte_rate
```

## Paso 5 — Distribución de espacio
```bash
docker exec broker1 /opt/kafka/bin/kafka-log-dirs.sh --bootstrap-server broker1:19092 --describe --broker-list 1,2,3 --topic-list dian.facturacion.emitida | tail -1 | python3 -m json.tool | grep -E '"partition"|"size"' | head -12
```
**Observe** el tamaño por partición y por broker; con RF=3 cada partición aparece en los tres.

## Paso 6 — Autocreación deshabilitada ★
```bash
echo "x" | docker exec -i broker1 /opt/kafka/bin/kafka-console-producer.sh --bootstrap-server broker1:19092 --topic topic.que.no.existe
kt --list | grep "no.existe" || echo "no se creó: correcto"
```
**Salida esperada:** `WARN … UNKNOWN_TOPIC_OR_PARTITION` repetido y luego error; el topic **no** aparece en la lista. Ese es el comportamiento deseado por gobierno.

## Informe (media página)
Salidas de los pasos ★ y respuesta a: **(a)** ¿qué parámetro habría que cambiar, en qué nivel y con qué herramienta, para que la retención de `dian.facturacion.emitida` pase a 45 días sin reiniciar? **(b)** ¿Por qué la cuota no produce errores en el cliente sino solo lentitud? **(c)** ¿por qué en AWS compartido usamos topics con prefijo por equipo?
