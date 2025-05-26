from faker import Faker
import pandas as pd
import random

# Inicializar Faker
fake = Faker()

# Lista de comidas favoritas
comidas_favoritas = ["pizza", "pasta", "sushi", "tacos", "hamburguesa", "ensalada", "pollo", "chocolate", "helado", "fruta"]

# Generar datos
def generar_datos(n):
    datos = []
    for _ in range(n):
        nombre = fake.name()
        nickname = fake.user_name()
        pais = fake.country()
        comida_fav = random.choice(comidas_favoritas)
        numero_suerte = random.randint(1, 100)
        datos.append([nombre, nickname, pais, comida_fav, numero_suerte])
    return datos

# Crear DataFrame y guardarlo como CSV
def main():
    print("Generando datos...")
    datos = generar_datos(100000)
    df = pd.DataFrame(datos, columns=["nombre", "nickname", "pais", "comida_favorita", "numero_de_la_suerte"])
    df.to_csv("nuevos_datos.csv", index=False)
    print("Archivo 'nuevos_datos.csv' creado con éxito.")

if __name__ == "__main__":
    main()
