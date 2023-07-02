# importar de outro arquivo
from extract import *
from transform import *
from load import *
# importando bibliotecas
import time

##########################################################################
#                            Definir variáveis                           #
##########################################################################
file_key = "keys\dw-ufsc-390713-8946216796dd.json"
dataset_name = "caged"
data_folder = "dados"
# Registrar o tempo no início da execução
start_time = time.time()

##########################################################################
#               Criar conexão com o GCP, dataset e tabelas               #
##########################################################################
# Conexão com GCP
client, credentials = gcp_connection(file_key)
# Verificar se o dataset já existe, se não existe, cria
dataset_fonte = dataset_exist(client,dataset_name)
# Verifica se as tabelas já existem, se não existe, cria
table_movimentacao, table_localidade, table_trabalhador, table_periodo, table_empregador, table_fato_caged = table_exist(client,dataset_fonte)

##########################################################################
#           Criar dimensão localidade, se ela ainda não existir          #
##########################################################################
if not check_data(client,table_localidade):
    df_localidade = create_df_localidade(data_folder)
    load_data({table_localidade:df_localidade},client,dataset_fonte)
else:
    print("--------------------------------------------------------------------------")
    print("Dados da tabela dw-caged.caged.dim_localidade já carregados no GCP.")

##########################################################################
#                               Executar ETL                             #
##########################################################################
while True:

    ##########################################################################
    #                             Extrair dados                              #
    ##########################################################################
    # Verificar próximo ano e mês a baixar dados
    proximo_ano, proximo_mes = update_date(client,credentials,dataset_fonte,table_periodo)
    # Baixar arquivos
    if not download_files(proximo_ano,proximo_mes,data_folder):
        break
        
    ##########################################################################
    #                         Executar transformações                        #
    ##########################################################################
    # Ler todos os arquivos, tratar e agrupar em um único dataframe
    df_group = group_files(data_folder)
    # Criar dfs da fato e das dimensões
    df_movimentacao, df_trabalhador, df_periodo, df_empregador, df_fato_caged = create_dfs(df_group)

    ##########################################################################
    #                          Carregar dados no GCP                         #
    ##########################################################################
    # Incluir tabelas e dfs em uma biblioteca
    tables_dfs = {
        table_movimentacao:df_movimentacao,
        table_trabalhador:df_trabalhador,
        table_periodo:df_periodo,
        table_empregador:df_empregador,
        table_fato_caged:df_fato_caged}
    # Carregar os dados no GCP
    load_data(tables_dfs,client,dataset_fonte)

##########################################################################
#                       Calculando tempo de execução                     #
##########################################################################
# Registrar o tempo no final da execução
end_time = time.time()
# Calcular o tempo total de execução
total_time = end_time - start_time

print("--------------------------------------------------------------------------")
print("##########################################################################")
print("#                       Fim da execução do programa                      #")
print("##########################################################################")

# Exibir o tempo total de execução
print("--------------------------------------------------------------------------")
print(f"Tempo total de execução: {total_time} segundos.")
print("--------------------------------------------------------------------------")