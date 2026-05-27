# Proyecto: Data Lake de Alta Disponibilidad con MinIO

Este proyecto es una extensión del pipeline de MLOps de Calidad del Aire. Su objetivo es transformar el almacenamiento efímero actual en un **Data Lake persistente, escalable y con alta disponibilidad (HA)** utilizando MinIO en modo distribuido sobre Kubernetes.

---

## 1. Visión General

El sistema actual utiliza un único Pod de MinIO con `emptyDir`, lo que significa que los datos se pierden si el Pod se reinicia o el nodo falla. Esta extensión implementa un **Cluster MinIO Distribuido** que utiliza *Erasure Coding* para garantizar la redundancia y la integridad de los datos incluso ante el fallo de varios nodos o discos.

### Objetivos Clave:
- **Persistencia:** Uso de `PersistentVolumeClaims` (PVC) en lugar de memoria efímera.
- **Redundancia:** Configuración de *Erasure Coding* (mínimo 4 instancias).
- **Escalabilidad:** Arquitectura basada en `StatefulSet` para gestionar identidades de red estables.
- **Independencia:** Funciona como un servicio de infraestructura centralizado para múltiples pipelines.

---

## 2. Arquitectura Técnica

### 2.1 Modo Distribuido y Erasure Coding
MinIO distribuido permite combinar múltiples discos (en diferentes Pods) en un único objeto de almacenamiento. 
- **Configuración recomendada:** 4 Replicas (nodos).
- **Erasure Coding:** Divide los objetos en fragmentos de datos y de paridad. Con 4 nodos, el sistema puede tolerar la pérdida de hasta 2 nodos para lectura y 1 para escritura (dependiendo de la configuración de paridad).

### 2.2 Componentes de Kubernetes
| Recurso | Función |
|---|---|
| **StatefulSet** | Gestiona los Pods de MinIO (`minio-0` a `minio-3`). Proporciona nombres DNS estables y vinculación determinista con los volúmenes. |
| **Headless Service** | Permite la comunicación interna entre los nodos del cluster para la sincronización de datos. |
| **LoadBalancer / NodePort** | Expone la API (S3) y la Consola al exterior del cluster. |
| **PersistentVolumeClaim (PVC)** | Solicita almacenamiento persistente al cluster (vía StorageClass). |

---

## 3. Guía de Implementación

### Paso 1: Definición del Almacenamiento
Se debe configurar un `volumeClaimTemplates` en el `StatefulSet` para que cada Pod reciba su propio disco persistente.

### Paso 2: Configuración del Cluster
El comando de arranque de MinIO debe incluir los endpoints de todos los nodos:
```bash
minio server http://minio-{0...3}.minio-svc.default.svc.cluster.local/data
```

### Paso 3: Seguridad y Credenciales
Uso de `Secrets` de Kubernetes para gestionar las `ROOT_USER` y `ROOT_PASSWORD`, además de certificados TLS para encriptación en tránsito si se requiere producción.

---

## 4. Guía de Ejecución

Para levantar todo el sistema desde cero en un entorno local, se ha creado un script maestro que automatiza los parches necesarios:

```bash
./start-all.sh
```

### 4.1 Problemas Resueltos en el Despliegue
- **Resolución DNS:** Se ha detectado que el driver Docker de Minikube a veces tiene problemas de DNS (`server misbehaving`). El script fuerza el uso de `8.8.8.8` en el nodo.
- **Sincronización de MinIO:** El modo distribuido requiere que los 4 nodos se reconozcan antes de aceptar comandos. Se ha implementado un bucle de reintentos en `deploy-all.sh`.
- **Compatibilidad de Nombres:** Los Jobs originales esperaban `minio-credentials` y `minio-config`. Se han unificado los nombres en `minio-ha.yaml` para evitar fallos de configuración en los contenedores.

---

## 5. Mantenimiento y Operación

- **Monitoreo:** MinIO expone métricas compatibles con Prometheus en el endpoint `/minio/v2/metrics/cluster`.
- **Backup:** Aunque sea HA, se recomienda realizar backups periódicos a un almacenamiento fuera del cluster (ej. AWS S3 real o disco duro externo).
- **Actualizaciones:** El modo `StatefulSet` permite realizar actualizaciones *rolling updates* de una en una, manteniendo la disponibilidad del servicio.
