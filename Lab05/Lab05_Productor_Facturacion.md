# Laboratorio 05 — Productor de eventos de facturación
**Sesión 7** · Módulo 4.1 · 40 min guiados en vivo · Entregable: código en rama personal + informe de mediciones (jueves 15/10)

## Objetivo
Implementar un productor con clave = NIT, medir el rendimiento con `acks` 0/1/all, construir una matriz de `linger.ms` × compresión, provocar la caída del líder durante el envío y verificar la ausencia de duplicados con idempotencia.

## Prerrequisitos
- Entorno en ejecución en AWS/code-server. Las dependencias Python deben estar instaladas en la instancia; si existe `.venv-kafka`, actívelo con `source .venv-kafka/bin/activate`.
- Archivos de `codigo/`: `productor_facturacion.py` (esqueleto), `bench.py`. Los comandos siguientes se ejecutan desde la raíz del repositorio.
- Topic de trabajo por equipo:
  ```bash
  cd ~/kafka-dian
  EQUIPO=equipo01
  TOPIC=$EQUIPO.lab05.facturacion.emitida
  docker exec broker1 /opt/kafka/bin/kafka-topics.sh --bootstrap-server broker1:19092 \
    --create --topic "$TOPIC" --partitions 6 --replication-factor 3 --config min.insync.replicas=2
  ```
- Cree su rama: `git checkout -b lab05-<usuario>`.
- Los pasos que detienen brokers afectan a todos los participantes; ejecútelos solo cuando el docente lo indique.

## Paso 1 — Completar el esqueleto ★
Abra `productor_facturacion.py` y resuelva los `TODO` del paso 1: añada `batch.size` (p. ej. 65536) y `delivery.timeout.ms` (120000) a la configuración y confirme que la clave enviada es el NIT. Ejecute:
```bash
python3 codigo/productor_facturacion.py --topic "$TOPIC" --n 2000 --acks all
```
**Salida esperada:** `… -> 2000 eventos en X s = N ev/s, errores=0`.
Verifique el orden por clave: consuma con `--property print.key=true --property print.partition=true` y compruebe que un mismo NIT siempre aparece en la misma partición.

## Paso 2 — Rendimiento según acks ★
```bash
for a in 0 1 all; do python3 codigo/productor_facturacion.py --topic "$TOPIC" --n 20000 --acks $a; done
```
Registre los tres valores. Lo esperado: `acks=0` > `acks=1` > `acks=all`, con una diferencia moderada en un entorno local (la red es la del contenedor).

## Paso 3 — Matriz linger.ms × compresión ★
```bash
python3 codigo/bench.py --topic "$TOPIC" --n 20000 > mediciones.md
docker exec broker1 du -sh /var/lib/kafka/data/${TOPIC}-*   # tamaño en disco
```
`bench.py` imprime las tablas en Markdown listas para el informe. Para el tamaño en disco por combinación, borre y recree el topic entre combinaciones o use topics distintos con prefijo (`--topic $EQUIPO.lab05.zstd`, etc.). Registre la CPU de la instancia o la ventana de ejecución asignada.

## Paso 4 — Caída del líder durante el envío ★
1. Identifique el líder de la partición 0: `docker exec broker1 /opt/kafka/bin/kafka-topics.sh --bootstrap-server broker1:19092 --describe --topic "$TOPIC"`.
2. Lance un envío largo **sin** idempotencia: `python3 codigo/productor_facturacion.py --topic "$TOPIC" --n 200000 --acks all --linger 20`.
3. A los ~5 s, en otra terminal: `docker stop brokerN` (el líder de P0).
4. Observe la salida: mensajes `[ERROR] … reintentable=True` (p. ej. `NOT_LEADER_OR_FOLLOWER`) que el cliente reintenta solo, y eventualmente el envío termina con `errores=0` o con unos pocos si venció `delivery.timeout.ms`.
5. `docker start brokerN`.
Anote en el informe qué códigos de error vio y si fueron reintentables.

## Paso 5 — Idempotencia y conteo de duplicados ★
Repita el paso 4 con `--idempotence` y un topic limpio (`$EQUIPO.lab05.idem`). Luego cuente eventos únicos frente a totales:
```bash
TOPIC_IDEM=$EQUIPO.lab05.idem
docker exec broker1 /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server broker1:19092 --topic "$TOPIC_IDEM" --from-beginning --timeout-ms 20000 2>/dev/null \
  | python3 -c "import sys,json; ids=[json.loads(l)['id_evento'] for l in sys.stdin if l.strip()]; print('total',len(ids),'unicos',len(set(ids)))"
```
**Salida esperada:** `total N unicos N` (sin duplicados). Repita el mismo conteo sobre el topic del paso 4 (sin idempotencia): si hubo reintentos tras confirmaciones perdidas, `total > unicos`. Si no aparecen duplicados en su equipo, explique en el informe por qué (la ventana de reintento tras una confirmación perdida es corta en un entorno local).

## Reto opcional (perfil desarrollo) — Escritura transaccional
Implemente `productor_transaccional.py` que publique en `$EQUIPO.lab05.declaracion` y `$EQUIPO.lab05.liquidacion` dentro de una transacción (`transactional.id="$EQUIPO-lab05-tx-1"`, `init_transactions`, `begin_transaction`, `commit_transaction`). Aborte la segunda transacción (`abort_transaction`) y consuma con `--isolation-level read_committed`: los mensajes abortados no deben aparecer; con `read_uncommitted`, sí.

## Informe (1–2 páginas)
1. Tabla del paso 2 y matriz del paso 3 con tamaño en disco y CPU del equipo.
2. Análisis: ¿qué combinación elegiría para ingesta masiva de facturación y cuál para un flujo de alertas? Justifique con sus cifras.
3. Errores observados en el paso 4 y comportamiento del cliente.
4. Conteo del paso 5 y explicación.
5. Enlace a la rama con el código.
