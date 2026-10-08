# Laboratorio 10 — Agregación en tiempo real con Kafka Streams
**Sesión 13** · Módulo 5.4 · 35 min guiados en vivo · Informe (jueves 05/11)

## Objetivo
Contar facturas por NIT en ventanas de una hora y sumar el valor facturado; inspeccionar los topics internos; verificar la recuperación del estado tras una caída; y observar el efecto del periodo de gracia con eventos tardíos.

## Prerrequisitos
Etapa C en AWS/code-server. La ruta oficial es la aplicación Java en `labs/10-streams/`; requiere JDK 17 y Maven 3.9 o superior, disponibles en la instancia. La alternativa `codigo/streams_conteo_por_nit.py` sirve únicamente para discutir la lógica de una agregación: **no** reemplaza Kafka Streams ni evidencia changelog, repartición o restauración de estado.

Desde la raíz del repositorio, genere tráfico actual:
```bash
cd ~/kafka-dian
python3 codigo/gen_eventos.py --topic dian.facturacion.emitida --rate 100 --minutes 5 &
```

## Paso 1 — Topología base ★
```bash
cd labs/10-streams
mvn -q clean test package
java -jar target/conteo-nit-hora.jar
```
**Salida esperada:** la aplicación arranca, imprime la topología y comienza a emitir. Copie la topología impresa al informe e identifique el store `conteo-por-nit` y la repartición causada por `selectKey` + `groupByKey`.

## Paso 2 — Añadir la suma de valor ★
La aplicación entregada ya emite `{conteo, valor_total}` por NIT y ventana mediante `aggregate()`. Verifique la salida:
```bash
docker exec broker1 /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server broker1:19092 \
  --topic dian.facturacion.conteo-por-nit-hora --from-beginning --max-messages 5 --property print.key=true 2>/dev/null
```

## Paso 3 — Topics internos ★
```bash
docker exec broker1 /opt/kafka/bin/kafka-topics.sh --bootstrap-server broker1:19092 --list | grep -E "changelog|repartition"
docker exec broker1 /opt/kafka/bin/kafka-topics.sh --bootstrap-server broker1:19092 \
  --describe --topic <nombre-del-changelog> | head -1
```
**Observe:** el changelog es `cleanup.policy=compact` (conserva el último valor por clave: es el respaldo del almacén de estado) y el de repartición es `delete` (es tráfico transitorio).
**Para el informe:** explique la función de cada uno y por qué el changelog está compactado.

## Paso 4 — Caída y recuperación del estado ★
1. Anote el conteo actual de un NIT concreto de la salida.
2. Detenga la aplicación (Ctrl+C) a mitad del procesamiento.
3. Elimine el directorio de estado local para simular que arranca en **otro nodo**: `rm -rf /tmp/kafka-streams/conteo-nit-hora`.
4. Reinicie la aplicación.
**Observe:** en el arranque, `Restoring state from changelog`; los conteos continúan desde donde iban, no desde cero. El estado local se reconstruyó desde Kafka.

## Paso 5 — Eventos tardíos y periodo de gracia ★
Inyecte eventos con marca de tiempo 90 minutos en el pasado:
```bash
cd ~/kafka-dian
python3 codigo/gen_eventos.py --topic dian.facturacion.emitida --n 20 --retraso-min 90
```
**Observe:** primero debe existir tráfico actual para que el tiempo de flujo avance. Con la gracia por defecto de 10 minutos, los eventos de 90 minutos atrás llegan fuera de una ventana ya cerrada. Aumente la gracia, reinicie y repita:
```bash
GRACIA_MINUTOS=120 java -jar target/conteo-nit-hora.jar
```
**Para el informe:** ¿por qué en un flujo tributario descartar un evento tardío en silencio es inaceptable, y qué haría con los que superen la gracia?

## Paso 6 — Comparación de enfoques
ksqlDB no forma parte de la versión `v1.0.0-rc1`: no hay un servicio ni una licencia/configuración probada en el entorno. Para el informe compare la implementación Kafka Streams con el script Python didáctico: ¿qué se pierde si no existen topics internos, tienda de estado y restauración automática? La comparación con ksqlDB solo se habilitará cuando exista una etapa versionada y probada.

## Informe
Salidas de los pasos ★, topología del paso 1, explicación de los topics internos, evidencia de la recuperación, tratamiento de los eventos tardíos y la comparación con ksqlDB.
