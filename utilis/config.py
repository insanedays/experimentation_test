import json
from pathlib import Path
from pyspark.sql import SparkSession

try: 
    spark
except NameError:
    spark = SparkSession.builder.getOrCreate()

def load_config(config_path="../env.json"):
    """
    Lê o arquivo env.json e retorna o conteúdo como dicionário.
    """
    with open(Path(config_path)) as f:
        return json.load(f)

def get_paths() -> dict:
    """
    Carrega a configuração e retorna os caminhos montados por camada.
    """
    loaded = load_config()
    base = f"/Volumes/{loaded['CATALOG']}/{loaded['SCHEMA']}"
    return {
        "RAW_DIR": f"{base}/{loaded['RAW_VOLUME']}",
        "REFINED_DIR": f"{base}/{loaded['REFINED_VOLUME']}",
        "CURATED_DIR": f"{base}/{loaded['CURATED_VOLUME']}",
        "TMP_DIR": loaded.get("TMP_DIR", "/local_disk0/tmp/")
    }

def create_volumes_from_config(spark):
    """
    Carrega a configuração e cria o schema + volumes definidos.
    """
    loaded = load_config()
    catalog = loaded["CATALOG"]
    schema = loaded["SCHEMA"]
    volumes = {
        "RAW": loaded["RAW_VOLUME"],
        "REFINED": loaded["REFINED_VOLUME"],
        "CURATED": loaded["CURATED_VOLUME"]
    }

    print(f"[Config] Criando schema '{schema}' no catálogo '{catalog}'...")
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.{schema}")
    
    for label, volume in volumes.items():
        print(f"[Config] Criando volume '{volume}' ({label})...")
        spark.sql(f"""
            CREATE VOLUME IF NOT EXISTS {catalog}.{schema}.{volume}
            COMMENT 'Volume de camada {label.lower()} criado automaticamente'
        """)

    print("[Config] Todos os volumes foram verificados ou criados com sucesso.")

if __name__ == "__main__":
    print("Inicializando ambiente do projeto...")
    loaded = load_config()
    paths = get_paths()
    globals().update(paths)
    create_volumes_from_config(spark)

    print(" Diretórios configurados:")
    for k, v in paths.items():
        print(f"{k}: {v}")


