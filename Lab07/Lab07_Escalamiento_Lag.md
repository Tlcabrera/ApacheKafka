# Laboratorio 07 — Escalamiento de consumidores y medición de lag
**Sesión 9** · Módulo 4.3 · 35 min guiados en vivo · Informe (jueves 22/10)

## Objetivo
Medir el lag con un consumidor, escalar a 3 y 6, verificar que un séptimo queda ocioso, comparar la interrupción del consumo entre asignadores eager y cooperativo, y provocar y corregir un ciclo de expulsión por poll lento.

## Prerrequisitos
Entorno + PostgreSQL en AWS/code-server. Use el topic del equipo creado en los laboratorios anteriores:
```bash
cd ~/kafka-dian
EQUIPO=equipo01
TOPIC=$EQUIPO.lab05.facturacion.emitida
GROUP=${EQUIPO}-lab07
```
Alias:
```bash
alias kg='docker exec broker1 /opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server broker1:19092'
```
Añada al esqueleto del consumidor un retardo artificial por mensaje para que el lag sea visible: exporte `SLEEP_MS=5` y en el bucle haga `time.sleep(int(os.getenv("SLEEP_MS","0"))/1000)` tras procesar (dos líneas; forma parte del ejercicio).

## Paso 1 — Producción continua
Terminal A:
```bash
python3 codigo/gen_eventos.py --topic "$TOPIC" --rate 500 --minutes 12
```

## Paso 2 — Un consumidor; medir el lag cada 30 s ★
Terminal B: `SLEEP_MS=5 python3 codigo/consumidor_facturacion.py --topic "$TOPIC" --group "$GROUP" --upsert`
Terminal C:
```bash
for i in $(seq 1 6); do date +%T; kg --describe --group "$GROUP" | awk 'NR>1 {lag+=$6} END {print "LAG TOTAL:", lag}'; sleep 30; done
```
**Esperado:** un consumidor a 5 ms/mensaje procesa ~200 ev/s frente a 500 producidos → el lag total crece ~300 por segundo. Registre la serie.

## Paso 3 — Escalar a 3 y a 6 ★
Abra dos terminales más con el mismo comando del paso 2 (mismo `--group "$GROUP"`). Repita la medición 3 minutos. Luego abra tres más (6 en total). Registre:
- `kg --describe --group "$GROUP"` → columna `CONSUMER-ID` por partición: la asignación cambia de 6/1 a 2/2/2 y luego 1/1/1/1/1/1.
- Serie de lag: con 3 consumidores (~600 ev/s) el lag deja de crecer; con 6 se recupera rápidamente.

## Paso 4 — Séptimo consumidor ★
Abra uno más. `kg --describe --group "$GROUP" --members --verbose`: el séptimo aparece con `#PARTITIONS = 0`. Anótelo.

## Paso 5 — Eager vs. cooperativo ★
Detenga todos los consumidores. Repita el escalado 3 → 6 dos veces:
- **A)** con `partition.assignment.strategy=range` (edite la configuración del esqueleto),
- **B)** con `cooperative-sticky` (valor del esqueleto).
En cada caso mida la interrupción: en la terminal del generador no cambia nada; en un consumidor ya activo, mida los segundos entre el último mensaje procesado antes del rebalanceo y el primero después (añada `print(time.time())` por mensaje o mire las marcas de tiempo de la BD: `SELECT max(procesado_en) - min(procesado_en) ...`). Con *range* todos los consumidores se detienen durante el rebalanceo; con *cooperative-sticky* los que no cambian de particiones siguen procesando.

## Paso 6 — Ciclo de expulsión por poll lento ★
Con 2 consumidores activos, arranque un tercero con `SLEEP_MS=400000` (más que `max.poll.interval.ms=300000`) y `max.poll.records` por defecto. Observe en su log:
`Application maximum poll interval (300000ms) exceeded by …` → el coordinador lo expulsa, rebalancea, y al volver a hacer poll se reincorpora… y vuelve a ser expulsado. Verifique con `kg --describe --group "$GROUP" --state` los cambios de `STATE` (Stable ↔ PreparingRebalance).
**Corrección:** añada `"max.poll.records": 1` a la configuración del consumidor y reduzca `SLEEP_MS` a 2000. El consumidor lento ahora respeta el contrato del poll: procesa 1 mensaje cada 2 s sin ser expulsado.

## Informe
1. Series de lag de los pasos 2 y 3 (tabla o gráfico) y la asignación de particiones en cada etapa.
2. Evidencia del séptimo consumidor sin particiones.
3. Segundos de interrupción medidos con *range* y con *cooperative-sticky*.
4. Log del ciclo de expulsión y la corrección aplicada; explique por qué elevar `max.poll.interval.ms` no era la solución correcta.
5. Relación con un flujo de su dependencia: ¿cuántas particiones y consumidores propondría y qué umbral de lag alertaría?
