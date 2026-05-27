# Memoria del Proyecto: Pipeline MLOps de Calidad del Aire de Valencia

**Asignatura:** Cloud & Big Data (CBD) — Trimestre 3  
**Fecha:** Mayo 2026  
**Dataset:** Datos horarios de calidad del aire del Ayuntamiento de Valencia (2016–2021)

---

## 1. Objetivo

Construir un pipeline **end-to-end** de MLOps que:

1. Descargue datos reales de calidad del aire de la API pública del Ayuntamiento de Valencia.
2. Aplique un ETL básico (limpieza e imputación de valores nulos).
3. Entrene un modelo de predicción y genere predicciones a 24 horas.
4. Visualice los resultados en una gráfica.
5. Ejecute cada etapa como un **Kubernetes Job** independiente.
6. Almacene todos los artefactos intermedios y finales en **MinIO** (object storage compatible con S3).

> El énfasis no está en el rendimiento del modelo, sino en demostrar el **flujo completo** de un pipeline de datos orquestado sobre Kubernetes.

---

## 2. Arquitectura General

```
API Valencia
     │
     ▼
[Job 1: Ingest] ──► [Data Lake HA (MinIO 4-nodes)] ◄── [Job 2: Impute]
                         │                 │
                         ▼                 ▼
               (raw.parquet)        (clean.parquet)
                         │                 │
                         └───────┬─────────┘
                                 ▼
                          [Job 3: Train] ──► [Data Lake HA]
                                                   │
                                                   ▼
                                            [Job 4: Plot]
```

### 2.1 Extensión: Data Lake de Alta Disponibilidad
Se ha implementado un segundo proyecto independiente (`datalake-ha/`) que gestiona un cluster distribuido de MinIO. 
- **Persistencia:** Reemplaza `emptyDir` por `PersistentVolumeClaims`.
- **Tolerancia a fallos:** 4 Pods en modo distribuido con *Erasure Coding*.
- **Integración:** El pipeline de Calidad del Aire se conecta a este cluster como su infraestructura de almacenamiento centralizada.


Todos los Jobs corren dentro de un **cluster Minikube** (Kubernetes local con driver Docker). MinIO actúa como capa de almacenamiento compartida entre Jobs — equivalente a S3 en un entorno cloud real.

---

## 3. Stack Tecnológico

| Capa | Tecnología | Por qué |
|---|---|---|
| Cluster local | **Minikube** (driver Docker) | Kubernetes local en un solo comando. Kind alternativa pero Minikube ofrece mejor integración con el daemon Docker para construir imágenes dentro del cluster. |
| Object storage | **MinIO** (S3-compatible) | Reemplaza NFS (incompatible con el kernel `linuxkit` de Docker Desktop). Protocolo HTTP/S3 sin dependencias de kernel. |
| Lenguaje | **Python 3.11** | Ecosistema ML maduro (pandas, scikit-learn). Versión estable con soporte a largo plazo. |
| Modelo ML | **LinearRegression** (scikit-learn) | Modelo interpretable que captura el patrón diario de NO₂. Entrena en < 1 segundo. Adecuado para una demo de flujo. |
| Contenedores | **python:3.11-slim** | Imagen base mínima, sin overhead. |
| Orquestación | **Kubernetes Jobs** | Primitiva nativa para cargas batch que terminan. Garantiza completitud y deja trazabilidad de éxito/fallo. |

---

## 4. Fuente de Datos

### 4.1 Corrección del spec original

La URL original del spec (`valencia.opendatasoft.com`) no existe. El portal real es **CKAN del Ayuntamiento de Valencia**:

```
https://opendata.vlci.valencia.es/es/dataset/b5c2656c-.../resource/4be7248b-.../download/rvvcca.-datos-horarios-valencia-2016-2021-curt-cas.csv
```

- Fichero CSV de ~47 MB con todas las estaciones y contaminantes (2016–2021).
- Descarga con `requests.get()` (no `pd.read_csv(URL)` — falla por SSL en macOS con el intérprete de `python.org`).

### 4.2 Dataset filtrado

| Parámetro | Valor |
|---|---|
| Estación | **Pista Silla** (≠ "Pista de Silla" — nombre incorrecto en el spec) |
| Contaminante | **NO₂** (µg/m³) |
| Ventana temporal | 2021-07-01 → 2021-12-31 |
| Total registros | 4 416 filas (184 días × 24 horas) |
| Nulos en NO₂ | ~4 % → imputados en Job 2 |

> El dataset está congelado en 2021-12-31 (aunque CKAN indica modificación en 2026 — solo metadatos). Ventana de 6 meses es suficiente para una demo.

### 4.3 Nombres de columnas con acentos

El CSV devuelve columnas **con tildes** (`Estación`, `Día`, `Presión`…). El filtrado correcto:

```python
df[df["Estación"] == "Pista Silla"]  # ✅
df[df["Estacion"] == "Pista Silla"]  # ❌ KeyError
```

---

## 5. Pipeline Detallado — Los 4 Jobs

### Job 1 · Ingest (`jobs/ingest/ingest.py`)

**Función:** Descarga y limpieza inicial.

1. Descarga el CSV completo (~47 MB) vía `requests`.
2. Filtra por `Estación == "Pista Silla"` y selecciona columna `NO2`.
3. Construye el timestamp horario combinando `Fecha` + `Hora`.
4. Recorta la ventana `2021-07-01 → 2021-12-31` con un buffer de 24 h anterior (necesario para que el lag-24 esté definido desde la primera hora útil).
5. Escribe `s3://aq-data/raw.parquet`.

**Variables de entorno requeridas:** `AQ_BUCKET`, `AWS_ENDPOINT_URL`, `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`.

---

### Job 2 · Impute (`jobs/impute/impute.py`)

**Función:** Imputación de valores nulos.

El ~4 % de nulos en NO₂ haría inválido el `lag-24` si no se rellenan antes de calcular features. Estrategia elegida: **media móvil retroactiva de los 4 valores anteriores**, en bucle hasta eliminar todos los NaN.

```
NaN = media de los 4 valores no nulos inmediatamente anteriores
→ iterar para rachas largas de NaN (usando valores ya imputados)
```

- Lee `s3://aq-data/raw.parquet`.
- Escribe `s3://aq-data/clean.parquet` (sin ningún NaN en NO₂).
- Aserta `n_after == 0` — falla ruidosamente si queda algún nulo.

> **Decisión de diseño:** mantener todas las filas (0 descartadas) vs. la alternativa de eliminar filas con NaN (perdería la continuidad temporal necesaria para el lag).

---

### Job 3 · Train (`jobs/train/train.py`)

**Función:** Feature engineering, entrenamiento y predicción.

**Features:**
| Feature | Descripción |
|---|---|
| `hour` | Hora del día (0–23) — captura el ciclo diario de tráfico |
| `dayofweek` | Día de la semana (0=lunes) — captura diferencias laborable/fin de semana |
| `no2_lag24` | Valor de NO₂ hace exactamente 24 horas — autocorrelación diaria |

**Proceso:**
1. Calcula features y descarta el buffer de 24 h inicial (primeras 24 filas sin `lag-24`).
2. Split temporal 80/20 (sin aleatorización — respeta el orden cronológico).
3. Entrena `LinearRegression` sobre el 80% más antiguo.
4. Evalúa con **MAE** sobre el 20% más reciente.
5. Genera predicciones para las **próximas 24 horas** usando el lag-24 del histórico real.

**Resultados obtenidos:**

| Métrica | Valor |
|---|---|
| MAE en test | **12.02 µg/m³** |
| Filas de entrenamiento | 3 532 |
| Filas de test | 884 |

- Escribe `dataset.parquet`, `predictions.csv` y `metrics.json` en MinIO.

---

### Job 4 · Plot (`jobs/plot/plot.py`)

**Función:** Visualización final.

Genera `forecast.png` con matplotlib:
- Serie observada de los **últimos 7 días** del histórico.
- Línea de **predicción 24 h** en rojo.
- Banda `± MAE` (12 µg/m³) sombreada — representa la incertidumbre del modelo.

Usa `matplotlib.use("Agg")` para renderizar sin display (necesario en contenedor sin X11).

---

## 6. Infraestructura Kubernetes

### 6.1 MinIO — Object Storage

Desplegado como `Deployment` dentro del cluster (`k8s/minio.yaml`):

- Imagen: `minio/minio:RELEASE.2024-10-13T13-34-11Z`
- Backing storage: `emptyDir` (ephemeral — los datos se pierden si se borra el Pod, coherente con el propósito académico).
- Expuesto internamente: `http://minio.default.svc.cluster.local:9000` (API S3) y `:9001` (consola web).
- Credenciales en `Secret` de Kubernetes (`minio-credentials`).
- Job `minio-init` crea el bucket `aq-data` al arrancar.

> **Por qué MinIO en lugar de NFS:** el kernel `linuxkit` de Docker Desktop no incluye módulos NFS, haciendo imposible montar un servidor NFS dentro del cluster con driver Docker. MinIO habla HTTP puro, sin dependencias de kernel.

### 6.2 Manifiestos de Jobs

Cada Job (`k8s/job-ingest.yaml`, `job-impute.yaml`, `job-train.yaml`, `job-plot.yaml`) sigue el mismo patrón:

```yaml
spec:
  ttlSecondsAfterFinished: 600  # se auto-borra a los 10 min
  backoffLimit: 1               # 1 reintento en caso de fallo
  template:
    spec:
      restartPolicy: Never
      containers:
        - image: aq-<job>:v1
          imagePullPolicy: Never  # usa imagen local de minikube
          envFrom:
            - secretRef:    { name: minio-credentials }
            - configMapRef: { name: minio-config }
```

`imagePullPolicy: Never` es crítico — indica a Kubernetes que no intente descargar la imagen de un registry externo (las imágenes se construyen directamente dentro del daemon Docker de Minikube).

### 6.3 Imágenes Docker

Todas siguen el mismo patrón mínimo:

```dockerfile
FROM python:3.11-slim
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY <job>.py .
CMD ["python", "<job>.py"]
```

Las imágenes se construyen **dentro del daemon Docker de Minikube** para estar disponibles en el cluster sin transferencia:

```bash
eval "$(minikube -p aq docker-env)"
docker build -t aq-ingest:v1 jobs/ingest/
```

---

## 7. Script de Operaciones (`aq.sh`)

Wrapper Bash que automatiza todas las operaciones del proyecto:

| Comando | Acción |
|---|---|
| `./aq.sh cluster` | Arranca el cluster Minikube (perfil `aq`, 4 CPUs, 24 GB RAM) |
| `./aq.sh minio` | Despliega MinIO y espera a que el bucket `aq-data` esté creado |
| `./aq.sh images` | Construye las 4 imágenes Docker dentro de Minikube |
| `./aq.sh run <job>` | Ejecuta un Job individual y muestra sus logs |
| `./aq.sh pipeline` | Ejecuta los 4 Jobs en orden secuencial |
| `./aq.sh up` | Todo de cero: `cluster + minio + images + pipeline` |
| `./aq.sh ls` | Lista los objetos del bucket `aq-data` |
| `./aq.sh cp <fichero>` | Descarga un objeto del bucket al directorio local |
| `./aq.sh reset` | Borra el cluster Minikube |

### Flujo completo de despliegue

```bash
# Prerrequisito: Docker Desktop abierto y running
./aq.sh up

# Para ver los artefactos generados:
./aq.sh ls
./aq.sh cp forecast.png
./aq.sh cp metrics.json
./aq.sh cp predictions.csv
```

---

## 8. Decisiones de Diseño Clave

### 8.1 4 Jobs en lugar de 3

El spec original planteaba 3 Jobs (Ingest+Transform, Train, Plot). Durante la validación del dataset se detectó que un ~4 % de valores NO₂ son nulos, lo que invalida el cálculo del `lag-24`. Se añadió el **Job 2 (Impute)** como etapa independiente:

- Separación de responsabilidades clara.
- Permite reejecutar solo la imputación si cambia la estrategia.
- Los datos crudos quedan preservados en `raw.parquet`.

### 8.2 MinIO vs. PVC con hostPath

El spec original proponía un PVC con `hostPath`. Se migró a MinIO porque:

- El kernel `linuxkit` de Docker Desktop no soporta módulos NFS.
- MinIO proporciona una interfaz S3 estándar (el mismo código funciona contra AWS S3 en producción cambiando solo el endpoint).
- Añade trazabilidad: se puede ver qué artefactos existen con `./aq.sh ls`.

### 8.3 Minikube en lugar de Kind

Aunque el spec original mencionaba Kind, se optó por Minikube porque permite apuntar directamente al daemon Docker interno del cluster (`minikube docker-env`), simplificando enormemente el proceso de build de imágenes sin necesidad de un registry privado.

### 8.4 Sin Airflow (versión actual)

El spec planteaba orquestar con Airflow + `KubernetesJobOperator`. En la implementación actual la orquestación la hace `aq.sh pipeline` (secuencia de `kubectl apply` con `kubectl wait`). Esta decisión simplifica el entorno local a cambio de perder la UI de Airflow y el scheduling automático — aceptable para una demo académica.

---

## 9. Resultados

| Artefacto | Ruta en MinIO | Descripción |
|---|---|---|
| `raw.parquet` | `s3://aq-data/raw.parquet` | Serie NO₂ cruda filtrada (con nulos) |
| `clean.parquet` | `s3://aq-data/clean.parquet` | Serie NO₂ imputada (sin nulos) |
| `dataset.parquet` | `s3://aq-data/dataset.parquet` | Dataset con features para entrenamiento |
| `predictions.csv` | `s3://aq-data/predictions.csv` | Predicciones para las próximas 24 h |
| `metrics.json` | `s3://aq-data/metrics.json` | `{"mae": 12.02, "n_train": 3532, "n_test": 884}` |
| `forecast.png` | `s3://aq-data/forecast.png` | Gráfica: histórico 7 días + predicción 24 h + banda ±MAE |

**MAE = 12.02 µg/m³** — error medio de predicción. El NO₂ en la estación Pista Silla oscila típicamente entre 5 y 60 µg/m³, por lo que un error de 12 µg/m³ es razonable para un modelo lineal sin tuning.

---

## 10. Problemas Encontrados y Soluciones

| Problema | Solución |
|---|---|
| URL de la API en el spec incorrecta (404) | Localizado el endpoint CKAN real del Ayuntamiento de Valencia |
| Nombre de estación incorrecto ("Pista de Silla") | Nombre correcto: `"Pista Silla"` — verificado consultando el CSV |
| Columnas con acentos en CSV vs. sin acentos en API JSON | Filtrar siempre con tildes al usar el CSV directo |
| `pd.read_csv(URL)` falla por SSL en macOS | Usar `requests.get(URL).content` + `BytesIO` |
| Dataset congelado en 2021 (no tiene datos recientes) | Ventana de 6 meses (jul–dic 2021) suficiente para la demo |
| Kernel `linuxkit` incompatible con NFS | Migración a MinIO (S3 por HTTP) |
| `~4 %` de nulos en NO₂ invalidan `lag-24` | Job 2 dedicado a imputación con media móvil retroactiva |
| Fallos de DNS en Minikube (`server misbehaving`) | Forzar DNS `8.8.8.8` en el nodo mediante SSH |
| MinIO HA tarda en sincronizar los 4 nodos | Implementado bucle de reintentos con `mc` en el despliegue |
| Conflicto de nombres en Secrets/ConfigMaps | Unificados nombres a `minio-credentials` para compatibilidad |

---

## 11. Estructura del Repositorio

```
air-quality/
├── aq.sh                    # Script principal de operaciones
├── PROJECT_SPEC_MINIMAL.md  # Especificación original del proyecto
├── PHASE_0_PREREQUISITES.md # Validación del entorno y correcciones al spec
├── data/                    # Datos de ejemplo / resultados locales
│   ├── metrics.json
│   ├── predictions.csv
│   └── rvvcca_raw.csv
├── jobs/                    # Código de cada Job
│   ├── ingest/              # Job 1: descarga y filtrado
│   ├── impute/              # Job 2: imputación de nulos
│   ├── train/               # Job 3: entrenamiento y predicción
│   └── plot/                # Job 4: visualización
├── k8s/                     # Manifiestos de Kubernetes
│   ├── minio.yaml           # Deployment MinIO + Service + Job init
│   ├── job-ingest.yaml
│   ├── job-impute.yaml
│   ├── job-train.yaml
│   └── job-plot.yaml
└── notebooks/
    └── exploration.ipynb    # Exploración inicial del dataset
```
