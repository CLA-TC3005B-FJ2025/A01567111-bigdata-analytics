from pyspark.sql import SparkSession
from pyspark.sql.functions import max, min, avg, count, desc, col

def crear_spark_session():
    spark = SparkSession.builder.appName("Analisis de Nuevos Datos").getOrCreate()
    return spark

def cargar_datos(spark, file_path):
    df = spark.read.option("header", "true").option("inferSchema", "true").csv(file_path)
    print("Datos cargados exitosamente:")
    df.show()
    return df

def main():
    spark = crear_spark_session()
    file_path = 'nuevos_datos.csv'
    df = cargar_datos(spark, file_path)

    # Total de registros
    print(f"\nNúmero total de registros: {df.count()}")

    # Registros por país (top 15)
    print("\nNúmero de registros por país:")
    df.groupBy("pais").count().orderBy("count", ascending=False).show(15)

    # Máximo y mínimo del número de la suerte
    print("\nMáximo y mínimo del número de la suerte:")
    df.select(max("numero_de_la_suerte"), min("numero_de_la_suerte")).show()

    # Filtrar registros con comida favorita 'pizza'
    print("\nRegistros con comida favorita 'pizza':")
    df.filter(df.comida_favorita == 'pizza').show(10)

    # Ordenar por número de la suerte descendente
    print("\nRegistros ordenados por número de la suerte (descendente):")
    df.orderBy(df.numero_de_la_suerte.desc()).show(10)

    # 🔁 CONSULTAS EXTRA

    # a. Número de registros por nickname
    print("\nNúmero de registros por nickname:")
    df.groupBy("nickname").count().orderBy("count", ascending=False).show(10)

    # b. Comida favorita más común
    print("\nComida favorita más común:")
    df.groupBy("comida_favorita").count().orderBy(desc("count")).show(1)

    # c. Promedio del número de la suerte por país
    print("\nPromedio del número de la suerte por país:")
    df.groupBy("pais").agg(avg("numero_de_la_suerte").alias("promedio_suerte")).orderBy(desc("promedio_suerte")).show(10)

    # d. Nickname más común
    print("\nNickname más común:")
    df.groupBy("nickname").count().orderBy(desc("count")).show(1)

    spark.stop()

if __name__ == "__main__":
    main()
