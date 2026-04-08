# Este Dockerfile es para construir una imagen de Docker que ejecute el procesador de eventos de la fábrica. 
# Utiliza una imagen base de Python 3.11, instala las dependencias necesarias desde el archivo requirements.txt, 
# copia el código fuente al contenedor y ejecuta el script processor.py al iniciar el contenedor.

FROM python:3.11
WORKDIR /app
COPY requirements.txt .
RUN pip install -r requirements.txt
COPY . .
CMD ["python", "/app/src/processor.py"]