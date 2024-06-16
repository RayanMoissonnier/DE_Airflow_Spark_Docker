# Utiliser une image de base Airflow avec la dernière version de Python et Airflow
FROM apache/airflow:2.9.2-python3.9

# Installer les dépendances nécessaires en tant que root
USER root
RUN apt-get update && apt-get install -y \
    build-essential \
    wget \
    openjdk-17-jdk-headless \
    && rm -rf /var/lib/apt/lists/*

# Configurer JAVA_HOME pour Spark
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
ENV PATH=$PATH:$JAVA_HOME/bin

# Télécharger et installer Spark
RUN wget https://archive.apache.org/dist/spark/spark-3.3.2/spark-3.3.2-bin-hadoop3.tgz && \
    tar -xvzf spark-3.3.2-bin-hadoop3.tgz && \
    mv spark-3.3.2-bin-hadoop3 /opt/spark && \
    rm spark-3.3.2-bin-hadoop3.tgz

# Configurer SPARK_HOME pour Spark
ENV SPARK_HOME=/opt/spark
ENV PATH=$PATH:$SPARK_HOME/bin

# Changer l'utilisateur à airflow pour installer les packages Python
USER airflow

# Installer pip packages
RUN pip install --no-cache-dir duckdb pandas requests pyspark

# Copier les fichiers de votre application dans le conteneur
COPY dags /opt/airflow/dags
