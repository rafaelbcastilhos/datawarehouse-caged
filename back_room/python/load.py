# importar bibliotecas
from google.cloud import bigquery
from google.oauth2 import service_account
import os


def gcp_connection(file_key):
    ##########################################################################
    #                        Cria a conexão com o GCP                        #
    ##########################################################################

    print("##########################################################################")
    print("#                     Iniciando execução do programa                     #")
    print("##########################################################################")
    print("--------------------------------------------------------------------------")
    print("Criando conexão com o GCP...")
    try:
        current_directory = os.path.dirname(os.path.abspath(__file__))
        file_path = os.path.join(current_directory, file_key)
        credentials = service_account.Credentials.from_service_account_file(file_path)
        client = bigquery.Client(credentials=credentials, project=credentials.project_id)
        print(f"Conexão realizada com sucesso com o projeto {credentials.project_id}.")
        print("--------------------------------------------------------------------------")
    except Exception:
        print(f"Não foi possível efetivar a conexão com o GCP.")
        print("--------------------------------------------------------------------------")
    return client,credentials

def dataset_exist(client,dataset_name):
    ##########################################################################
    #                     Cria o dataset caso não exista                     #
    ##########################################################################
    print("--------------------------------------------------------------------------")
    print(f"Verificando a existência do dataset {dataset_name} no GCP...")
    dataset_fonte = client.dataset(dataset_name)
    try:
        client.get_dataset(dataset_fonte)
        print(f"O conjunto de dados {dataset_fonte} já existe no GCP.")
        print("--------------------------------------------------------------------------")
    except Exception:
        print(f"Dataset {dataset_fonte} não foi encontrado no GCP, criando o dataset...")
        client.create_dataset(dataset_fonte)
        print(f"O conjunto de dados {dataset_fonte} foi criado no GCP com sucesso.")
        print("--------------------------------------------------------------------------")
    return dataset_fonte

def table_exist(client,dataset_fonte):
    ##########################################################################
    #                    Cria as tabelas caso não existam                    #
    ##########################################################################

    # Tabela e schema da tabela schema_movimentacao
    table_movimentacao = dataset_fonte.table("dim_movimentacao")

    schema_movimentacao = [
        bigquery.SchemaField("sk_movimentacao", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("is_tp_movimentacao", "STRING"),
        bigquery.SchemaField("is_categoria", "STRING"),
        bigquery.SchemaField("is_cbo_ocupacao", "STRING"),
        bigquery.SchemaField("is_fl_intermitente", "STRING"),
        bigquery.SchemaField("is_fl_trab_parcial", "STRING"),
        bigquery.SchemaField("is_fl_aprendiz", "STRING"),
        bigquery.SchemaField("is_fl_exclusao", "STRING"),
        bigquery.SchemaField("is_fl_fora_prazo", "STRING")
    ]

    # Tabela e schema da tabela dim_localidade
    table_localidade = dataset_fonte.table("dim_localidade")

    schema_localidade = [
        bigquery.SchemaField("sk_localidade", "INTEGER", mode="REQUIRED"),
        bigquery.SchemaField("is_regiao", "STRING"),
        bigquery.SchemaField("is_uf", "STRING"),
        bigquery.SchemaField("is_municipio", "STRING")
    ]

    # Tabela e schema da tabela dim_trabalhador
    table_trabalhador = dataset_fonte.table("dim_trabalhador")

    schema_trabalhador = [
        bigquery.SchemaField("sk_trabalhador", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("is_grau_instrucao", "STRING"),
        bigquery.SchemaField("is_genero", "STRING"),
        bigquery.SchemaField("nu_idade", "INTEGER"),
        bigquery.SchemaField("is_etnia", "STRING"),
        bigquery.SchemaField("is_tp_deficiencia", "STRING")
    ]

    # Tabela e schema da tabela dim_periodo
    table_periodo = dataset_fonte.table("dim_periodo")

    schema_periodo = [
        bigquery.SchemaField("sk_periodo", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("data_mov", "DATE"),
        bigquery.SchemaField("nu_ano", "INTEGER"),
        bigquery.SchemaField("nu_mes", "INTEGER")
    ]

    # Tabela e schema da tabela dim_empregador
    table_empregador = dataset_fonte.table("dim_empregador")

    schema_empregador = [
        bigquery.SchemaField("sk_empregador", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("is_tp_empregador", "STRING"),
        bigquery.SchemaField("is_tp_estabelecimento", "STRING"),
        bigquery.SchemaField("is_secao_econ", "STRING"),
        bigquery.SchemaField("is_subclasse_econ", "STRING"),
        bigquery.SchemaField("is_faixa_tam_estab", "STRING")
    ]

    # Tabela e schema da tabela fato_caged
    table_fato_caged = dataset_fonte.table("fato_caged")

    schema_fato_caged = [
        bigquery.SchemaField("sk_empregador", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("sk_periodo", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("sk_trabalhador", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("sk_movimentacao", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("sk_localidade", "INTEGER", mode="REQUIRED"),
        bigquery.SchemaField("vl_salario", "FLOAT"),
        bigquery.SchemaField("nu_hora_contratual", "FLOAT"),
        bigquery.SchemaField("nu_saldo", "INTEGER")
    ]

    tabelas = {
        table_movimentacao:schema_movimentacao,
        table_localidade:schema_localidade,
        table_trabalhador:schema_trabalhador,
        table_periodo:schema_periodo,
        table_empregador:schema_empregador,
        table_fato_caged:schema_fato_caged
    }

    print("--------------------------------------------------------------------------")
    print("Verificando a existência das tabelas no GCP...")
    print("--------------------------------------------------------------------------")
    for tabela, schema in tabelas.items():
        try:
            client.get_table(tabela, timeout=30)
            print(f"A tabela {tabela} já existe!")
            print("--------------------------------------------------------------------------")
        except:
            print(f"Tabela {tabela} não encontrada!")
            print(f"Criando tabela {tabela}...")
            client.create_table(bigquery.Table(tabela, schema=schema))
            print(f"A tabela {tabela} foi criada.")
            print("--------------------------------------------------------------------------")

    return table_movimentacao, table_localidade, table_trabalhador, table_periodo, table_empregador, table_fato_caged

def check_data(client,table_localidade):
    print("--------------------------------------------------------------------------")
    print(f"Verificando a existência de dados na tabela {table_localidade} do GCP...")
    # Obtenha informações sobre a tabela
    return client.get_table(table_localidade).num_rows > 0

def update_date(client,credentials,dataset_fonte,table_periodo):
    ##########################################################################
    #         Verifica a data de atualização para baixar os arquivos         #
    ##########################################################################
    # Construir a query
    query = f"""
        SELECT MAX(nu_mes) AS proximo_mes, nu_ano
        FROM `{credentials.project_id}.{dataset_fonte.dataset_id}.{table_periodo.table_id}`
        WHERE nu_ano = (
            SELECT MAX(nu_ano)
            FROM `{credentials.project_id}.{dataset_fonte.dataset_id}.{table_periodo.table_id}`
        )
        GROUP BY nu_ano
    """
    # Executar a query
    query_job = client.query(query)
    # Obter o resultado da consulta
    result = query_job.result().total_rows
    # Verificar se há dados na tabela
    if result > 0:
        # Obter os resultados
        results = query_job.result()
        # Iterar sobre os resultados
        for row in results:
            if int(row["proximo_mes"]) == 12:
                proximo_mes = 1
                proximo_ano = int(row["nu_ano"]) + 1
            else:
                proximo_mes = int(row["proximo_mes"]) + 1
                proximo_ano = int(row["nu_ano"])
    else:
        proximo_mes = 1
        proximo_ano = 2020
    return proximo_ano, proximo_mes

def load_data(tables_dfs,client,dataset_fonte):
    ##########################################################################
    #                         Carrega os dados no GCP                        #
    ##########################################################################
    print("--------------------------------------------------------------------------")
    print("Carregando dados no GCP...")
    for tabela, df in tables_dfs.items():
        table_ref = client.dataset(dataset_fonte.dataset_id).table(tabela.table_id)
        table_name = tabela.table_id
        job_config = bigquery.LoadJobConfig()
        job_config.write_disposition = bigquery.WriteDisposition.WRITE_APPEND
        
        # Verificar se o nome da tabela começa com "dim_"
        if table_name.startswith("dim_"):
            # Verificar duplicatas com base na coluna que começa com "sk_"
            existing_rows = client.list_rows(table_ref)
            existing_df = existing_rows.to_dataframe()
            existing_sk_values = existing_df[table_name.replace("dim_", "sk_")].tolist()
            
            # Filtrar novos dados removendo as duplicatas
            new_df = df[~df[table_name.replace("dim_", "sk_")].isin(existing_sk_values)]
            
            if new_df.empty:
                print(f"Não há novos dados para a tabela {table_name}.")
                continue
            
            # Carregar novos dados sem duplicatas
            job = client.load_table_from_dataframe(new_df, table_ref, job_config=job_config)
            job.result()
            print(f"Dados carregados na tabela {table_name}.")
        else:
            # Carregar dados diretamente sem verificar duplicatas
            job = client.load_table_from_dataframe(df, table_ref, job_config=job_config)
            job.result()
            print(f"Dados carregados na tabela {table_name}.")

