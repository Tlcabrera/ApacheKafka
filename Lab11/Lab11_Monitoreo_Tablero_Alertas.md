# Laboratorio 11 — Tablero de salud del clúster y alertas
**Sesión 14** · Módulo 6.1 · 40 min guiados en vivo · Entregable: JSON del tablero + capturas (martes 10/11)

## Objetivo
Instrumentar el clúster con Prometheus y Grafana, construir un tablero de seis paneles, provocar tres escenarios de incidente y definir tres reglas de alerta, disparando al menos una.

## Prerrequisitos
```bash
cd ~/kafka-dian/entorno
docker compose -f docker-compose-lab-kafka.yml -f docker-compose.etapaC.yml -f docker-compose.etapaD.yml up -d
```
La etapa D ya monta `entorno/jmx/jmx_prometheus_javaagent-1.6.0.jar` y `entorno/jmx/kafka.yml` en los tres brokers. No añada el agente manualmente ni configure `KAFKA_JMX_PORT` en los puertos 9101–9103: esos puertos son HTTP del exporter, no RMI. Verifique antes de continuar:
```bash
curl -fsS localhost:9101/metrics | grep kafka_server_replicamanager_underreplicatedpartitions
curl -fsS localhost:9102/metrics | grep kafka_controller_kafkacontroller_activecontrollercount
```

## Paso 1 — Verificar la recolección ★
Prometheus: http://35.153.139.120:9090 → Status → Targets. Los cuatro objetivos (`kafka-brokers` ×3 y `kafka-lag`) deben estar **UP**.
Consulta de prueba en Prometheus:
```
sum(kafka_server_replicamanager_underreplicatedpartitions)
```
**Salida esperada:** `0`.

## Paso 2 — Construir el tablero (seis paneles) ★
Grafana http://35.153.139.120:3000 (admin/admin) → añadir Prometheus como fuente (`http://prometheus:9090`). Cree los paneles con estas consultas:

| Panel | Consulta PromQL | Tipo |
|---|---|---|
| Particiones sub-replicadas | `sum(kafka_server_replicamanager_underreplicatedpartitions)` | Stat, umbral rojo > 0 |
| Particiones fuera de línea | `sum(kafka_controller_kafkacontroller_offlinepartitionscount)` | Stat, umbral rojo > 0 |
| Controlador activo | `sum(kafka_controller_kafkacontroller_activecontrollercount)` | Stat, verde = 1 |
| Latencia p99 de producción | `kafka_network_requestmetrics_produce_totaltimems_99th_percentile` | Serie temporal |
| Mensajes por segundo por topic | `sum by (topic) (rate(kafka_server_brokertopicmetrics_messagesin_total{topic!=""}[5m]))` | Serie temporal |
| Lag por grupo | `sum by (consumergroup) (kafka_consumergroup_lag)` | Serie temporal |

Construya al menos tres a mano antes de importar el tablero de referencia (`entorno/grafana/tablero-referencia.json`).

## Paso 3 — Escenario A: broker caído ★
```bash
docker stop broker2
```
Capture el tablero a los 30 s y a los 2 min. **Observe:** sub-replicadas > 0, el controlador sigue en 1 (si broker2 no era el controlador activo), y la latencia p99 sube. Reinicie: `docker start broker2` y capture la recuperación. Este escenario afecta a todo el grupo; ejecútelo solo cuando el docente lo indique.

## Paso 4 — Escenario B: consumidor detenido ★
Con el generador activo, detenga el consumidor del grupo `lab07`. **Observe:** el lag crece de forma lineal y sostenida mientras las métricas del broker permanecen normales.
**Punto clave para el informe:** el clúster está sano y el servicio está roto. Por eso las métricas de cliente son tan importantes como las del broker. Note además que el lag lo reporta `kafka-exporter`, no la aplicación caída.

## Paso 5 — Escenario C: producción por encima de la capacidad ★
```bash
cd ~/kafka-dian
python3 codigo/gen_eventos.py --topic dian.facturacion.emitida --rate 3000 --minutes 3
```
con un solo consumidor lento (`SLEEP_MS=10`). **Observe:** lag creciente y latencia p99 elevada simultáneamente. Compare con el escenario B: el diagnóstico es distinto.

## Paso 6 — Reglas de alerta ★
Use `alertas.yml` como base (ya está montado en Prometheus). Verifique en http://35.153.139.120:9090/alerts que las cinco reglas están cargadas. Provoque el disparo de `ParticionesSubReplicadas` deteniendo un broker más de 5 minutos y capture la alerta en estado **FIRING**. La alerta de espacio en disco no se incluye en este entorno porque requeriría un exporter del host; documente esa limitación y proponga una métrica de filesystem para producción.
Defina en Grafana **tres reglas propias** con umbral, duración, severidad y anotación de acción.

## Entregable
1. JSON del tablero exportado (Dashboard → Share → Export).
2. Capturas de los tres escenarios (antes / durante / después).
3. Captura de la alerta en FIRING.
4. Tabla de sus tres alertas propias: métrica, umbral, duración, severidad, destinatario y acción esperada. **Esta tabla es la entrada de la sección "Detección" del runbook del Taller 04.**
5. Respuesta: ¿por qué no conviene que la aplicación consumidora reporte su propio lag?
