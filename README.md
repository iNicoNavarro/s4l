# Solución propuesta

Este repositorio contiene la solución integral a una problema técnico que un **Ingeniero de Datos** debe lograr resolver. Se propone un entorno reproducible  y modular que simula un ecosistema de procesamiento de datos con PySpark y SQL Server, estructurado bajo un enfoque de lago de datos (Bronze, Silver, Gold).

## Estructura del proyecto

s4l/
├── README.md
├── requirements.txt
├── setup.sh
├── .env.example
├── venv/
├── docs/
│   ├── image/
│   │   ├── dim_patient.png
│   │   ├── dim_speciality.png
│   │   ├── dim_speciality_group.png
│   │   ├── dim_diagnosis.png
│   │   ├── dim_assignment_status.png
│   │   ├── dim_assistance_status.png
│   │   ├── fact_attention.png
│   │   ├── fact_patient_diagnosis.png
│   ├── datamodeler_modelo_logico_ERD.pdf
│   ├── modelo_logico_ERP_S4Ldrawio.png
│   └── Prueba Tecnica Ing Datos.pdf
├── solucion_puntos.md
├── puntos_scripts/
│   └── punto_3.py
├── data_source/
├── data_lake/
│   ├── bronze/
│   ├── silver/
│   └── gold/
├── sql_server/
│   ├── docker-compose.yml
│   └── init.sql
└── utils/
    ├── create_model_erp.py
    └── populate_model_erp.py

## Requisitos del entorno

* Python 3.10+
* pip
* Virtualenv (`python -m venv`)
* Docker (para SQL Server ficticio)
* PySpark (usado en notebooks)
* JupyterLab o VSCode
* java

## Configuración del entorno

1. Clonar el repositorio:

   * `git clone https://github.com/usuario/data-engineer-health-assessment.git`
   * `cd data-engineer-health-assessment`
2. Crear y activar el entorno virtual:

   * `python -m venv venv`
   * `source venv/bin/activate  # Linux/Mac`
3. Instalar dependencias:

   * `pip install -r requirements.txt `
4. Instalá OpenJDK 11 (es el más recomendado para Spark 3.x):

   * `sudo apt update`
   * `sudo apt install openjdk-11-jdk -y`
5. Ubicar la ruta de Java:

   * `readlink -f $(which java)`
   * Esto dará un path que hay que cargar (paso 6)
6. Añador en el archivo `.bashrc` (Linux) o `.zshrc` (Powershell) el `JAVA_HOME` (path obtenido en el paso anterior).

   * `nano ~/.zshrc`
7. Añadir al archivo al final la variable:

   * `export JAVA_HOME="path obtenido en el paso anterior"  `
   * `export PATH=$JAVA_HOME/bin:$PATH `
8. Aplicar los cambios:
   `source ~/.zshrc`
9. test: `python -c "from pyspark.sql import SparkSession; print(SparkSession.builder.master('local[*]').getOrCreate().version)"`

   Si todo está ok veremos algo así:
   `25/04/23 00:55:28 WARN Utils: Your hostname, NicoNavarro resolves to a loopback address: 127.0.1.1; using 10.255.255.254 instead (on interface lo) `
   `25/04/23 00:55:28 WARN Utils: Set SPARK_LOCAL_IP if you need to bind to another address  Setting default log level to "WARN".  To adjust logging level use sc.setLogLevel(newLevel).  For SparkR, use setLogLevel(newLevel). 25/04/23 00:55:29 WARN NativeCodeLoader: Unable to load native-hadoop library for your platform... using builtin-java classes where applicable  3.4.1`
10. (Opcional) Crear archivo `.env` basado en el `.env.example` si usas variables de entorno para configurar rutas, puertos, etc.
11. Iniciar el contenedor de SQL Server:
    `cd sql_server docker-compose up -d`
12. Levantar los notebooks para desarrollo:
    `jupyter lab  # o usar VSCode con extensión de Jupyter`

## Ejecución del proyecto

Los puntos de la prueba se resuelven progresivamente en módulos separados:

| Punto | Descripción                        | Archivo relacionado                         |
| ----- | ----------------------------------- | ------------------------------------------- |
| 1     | Mejora del modelo de datos ERP      | `docs/solucion_puntos.md`                 |
| 2     | Diseño del Data Lake               | `docs/solucion_puntos.md`                 |
| 3     | Modelo ERP en PySpark               | `notebooks/punto_3_modelo_erp.ipynb`      |
| 4     | Vista lógica ERP                   | `notebooks/punto_4_vista_logica.ipynb`    |
| 5     | Llaves naturales de la vista        | `docs/solucion_puntos.md`                 |
| 6     | Transformación Excel diagnósticos | `notebooks/punto_6_excel_transform.ipynb` |
| 7     | Preguntas sobre PySpark/Databricks  | `docs/solucion_puntos.md`                 |
| 8     | Notebook final de orquestación     | `notebooks/punto_8_etl_pipeline.ipynb`    |
