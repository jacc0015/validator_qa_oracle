📑 Data Validator Bot - Blackboard ETL
Esta herramienta automatiza la validación de integridad y volumetría entre las Vistas de Optimización (Origen) y las Tablas de Transición (Destino) en Oracle, específicamente para los procesos de Blackboard.

🚀 Características
Validación de Volumetría: Compara el COUNT(*) entre el origen y el destino.

Validación de Integridad: Utiliza el operador MINUS para detectar diferencias exactas en los datos, ignorando el orden de las filas.

Reportes Automáticos: Si se detectan discrepancias, genera un archivo .csv en la carpeta /logs detallando los registros fallidos.

Configuración Modular: No necesitas tocar el código Python para agregar nuevas validaciones; solo editas un archivo .yaml.

🛠️ Requisitos Previos
Python 3.8+ instalado.

Oracle Instant Client: Dado que trabajamos con Oracle 10g/11g, se recomienda tener instalado el Instant Client para evitar problemas de conexión.

Acceso de red a la base de datos desde la máquina donde se ejecute el script.

📥 Instalación
Clonar el repositorio:

Bash
git clone https://tu-repo-git/blackboard-validator.git
cd blackboard-validator
Instalar dependencias:

Bash
pip install -r requirements.txt
Configurar credenciales:
Crea un archivo llamado .env en la raíz del proyecto (este archivo está ignorado en Git por seguridad) y añade tus datos:

Fragmento de código
DB_USER=tu_usuario
DB_PASS=tu_password
DB_DSN=ip_servidor:1521/nombre_servicio
⚙️ Configuración de Validaciones
Para agregar o modificar las validaciones, edita el archivo config/queries.yaml.

Estructura del proceso:

nombre: Identificador del proceso para los logs.

vista_origen: La vista que creaste con la query optimizada.

tabla_destino: La tabla donde el SP inserta los datos.

llave_logica: La columna principal (ID) para ordenar los reportes de error.

🏃 Ejecución
Para correr la auditoría completa de los 8 procesos:

Bash
python validar_datos.py
¿Cómo leer los resultados?
En Consola: Verás un resumen visual. Si sale ✅ OK, los datos coinciden. Si sale ❌ FALLÓ, hay discrepancias.

En la carpeta /logs: Si un proceso falló, busca el archivo ERRORES_NombreProceso_Fecha.csv. Ábrelo en Excel para ver exactamente qué registros del origen no llegaron correctamente al destino.

⚠️ Notas Técnicas (Importante)
Duplicados: Si el conteo falla pero el MINUS no arroja registros, es muy probable que existan duplicados en la Vista que el SP está filtrando (o viceversa).

Nulls: El script considera que dos valores NULL en la misma columna son iguales.

Modo Thick: Si al conectar a Oracle 10g recibes errores de protocolo, activa el modo thick en el archivo validar_datos.py descomentando la línea de init_oracle_client.

## 💾 Gestión de Snapshots (Archivos Locales)

Este validador genera "fotos" del estado de la data en archivos CSV dentro de la carpeta `logs/`. 

### ¿Por qué guardamos todo en archivos?
1. **Evidencia:** Tienes pruebas físicas de qué contenía la Vista y la Tabla en cada ejecución.
2. **Análisis Offline:** Puedes abrir `SRC_*.csv` y `DST_*.csv` en Excel para comparar manualmente sin saturar la base de datos Oracle 19c.
3. **Histórico:** Permite mantener copias de seguridad de las cargas si el usuario decide no borrarlas.

### ⚠️ Advertencia de Espacio en Disco
* El script **NO elimina archivos automáticamente** para permitirte conservar tus históricos.
* Si realizas muchas pruebas con tablas pesadas (ej. Matrículas o Usuarios), monitorea el tamaño de la carpeta `logs/`.
* **Limpieza Manual:** Si te quedas sin espacio, simplemente elimina las carpetas `run_YYYYMMDD_HHMM` más antiguas que ya no necesites.

👥 Contribución
Si necesitas validar una nueva entidad (ej. grupos o anuncios), crea la vista en Oracle y simplemente regístrala en el archivo config/queries.yaml.