# importar bibliotecas
import os
import pandas as pd
import hashlib
# importar de outros arquivos
from constants import *


def create_df_localidade(data_folder):
    print("--------------------------------------------------------------------------")
    print("Criando o dataframe df_localidade...")
    # Ler arquivo de dados
    df_localidade = pd.read_csv(os.path.join(data_folder, 'MUNICIPIO.csv'), delimiter=';', encoding='ISO-8859-1')
    # Renomear as colunas
    df_localidade = df_localidade.rename(columns={
        'nm_regiao': 'is_regiao','nm_uf': 'is_uf','cd_mun_ibge_6': 'sk_localidade','nm_municipio': 'is_municipio'
        })
    # Excluir colunas não usadas
    df_localidade = df_localidade.drop(['cd_regiao','cd_uf','cd_mun_ibge_7'], axis=1)
    # Corrigir a sequência
    df_localidade = df_localidade.reindex(columns=['sk_localidade', 'is_regiao', 'is_uf','is_municipio'])
    print(f"Dataframe df_localidade criado com sucesso.")

    return df_localidade

def group_files(data_folder):

    # lista para armazenar os dataframes de cada arquivo CSV
    dfs = []

    print("--------------------------------------------------------------------------")
    print("--------------------------------------------------------------------------")
    print("lendo os dados dos arquivos extraídos, tratando e concatenando...")
    # Percorre todos os arquivos .txt na pasta
    for filename in os.listdir(data_folder):
        if filename.endswith('.txt'):
            # lê o arquivo como CSV com o pandas
            df = pd.read_csv(os.path.join(data_folder, filename), delimiter=';')

            # Verificar se o arquivo possui a coluna 'competênciaexc'
            if 'competênciaexc' not in df.columns:
                # Criar a coluna 'competênciaexc' com valor nulo
                df['competênciaexc'] = None

            # Verificar se o arquivo possui a coluna 'indicadordeexclusão'
            if 'indicadordeexclusão' not in df.columns:
                # Criar a coluna 'indicadordeexclusão' com valor 0
                df['indicadordeexclusão'] = 0
                
            # Criar a coluna data_mov
            df['competênciamov'] = df['competênciamov'].astype(str) + '01'
            df['data_mov'] = pd.to_datetime(df['competênciamov'], format='%Y%m%d')

            # criar colunas nu_ano e nu_mes
            df['nu_ano'] = df['data_mov'].dt.year
            df['nu_mes'] = df['data_mov'].dt.month

            # Alterar tipo de dados
            df['idade'] = df['idade'].fillna(0).astype(int)
            df['salário'] = df['salário'].str.replace(',', '.').fillna(0).astype(float)
            df['horascontratuais'] = df['horascontratuais'].str.replace(',', '.').fillna(0).astype(float)

            # Realizar de-para
            df['graudeinstrução'] = df['graudeinstrução'].astype(str).apply(lambda x: map_grau_instrucao.get(x, "Não Identificado"))
            df['sexo'] = df['sexo'].astype(str).apply(lambda x: map_genero.get(x, "Não Identificado"))
            df['raçacor'] = df['raçacor'].astype(str).apply(lambda x: map_etnia.get(x, "Não Identificado"))
            df['tipodedeficiência'] = df['tipodedeficiência'].astype(str).apply(lambda x: map_tp_deficiencia.get(x, "Não Identificado"))
            df['tipoempregador'] = df['tipoempregador'].astype(str).apply(lambda x: map_tp_empregador.get(x, "Não Identificado"))
            df['tipoestabelecimento'] = df['tipoestabelecimento'].astype(str).apply(lambda x: map_tp_estabelecimento[x])
            df['seção'] = df['seção'].astype(str).apply(lambda x: map_secao_economica.get(x, "Não Identificado"))
            df['subclasse'] = df['subclasse'].astype(str).apply(lambda x: map_subclasse_econ.get(x, "Não Identificado"))
            df['tamestabjan'] = df['tamestabjan'].astype(str).apply(lambda x: map_faixa_tam_estab.get(x, "Não Informado"))
            df['tipomovimentação'] = df['tipomovimentação'].astype(str).apply(lambda x: map_tp_movimentacao.get(x, "Não Identificado"))
            df['categoria'] = df['categoria'].astype(str).apply(lambda x: map_categoria.get(x, "Não Identificado"))
            df['cbo2002ocupação'] = df['cbo2002ocupação'].astype(str).apply(lambda x: map_cbo_ocupacao.get(x, "Não Identificado"))
            df['indtrabintermitente'] = df['indtrabintermitente'].astype(str).apply(lambda x: map_sim_nao.get(x, "Não Identificado"))
            df['indtrabparcial'] = df['indtrabparcial'].astype(str).apply(lambda x: map_sim_nao.get(x, "Não Identificado"))
            df['indicadoraprendiz'] = df['indicadoraprendiz'].astype(str).apply(lambda x: map_sim_nao.get(x, "Não Identificado"))
            df['indicadordeexclusão'] = df['indicadordeexclusão'].astype(str).apply(lambda x: map_sim_nao.get(x, "Não Identificado"))
            df['indicadordeforadoprazo'] = df['indicadordeforadoprazo'].astype(str).apply(lambda x: map_sim_nao.get(x, "Não Identificado"))

            # Concatenar as colunas do DataFrame
            concatenacao_trabalhador = df['graudeinstrução'] + df['sexo'] + df['idade'].astype(str) + df['raçacor'] + df['tipodedeficiência']
            # Calcular o hash para a concatenação das colunas
            hashes_trabalhador = concatenacao_trabalhador.apply(lambda x: hashlib.md5(x.encode()).hexdigest())
            # Adicionar a coluna 'hash' ao DataFrame
            df['sk_trabalhador'] = hashes_trabalhador

            # Concatenar as colunas do DataFrame
            concatenacao_empregador = df['tipoempregador'] + df['tipoestabelecimento'] + df['seção'] + df['subclasse'] + df['tamestabjan']
            # Calcular o hash para a concatenação das colunas
            hashes_empregador = concatenacao_empregador.apply(lambda x: hashlib.md5(x.encode()).hexdigest())
            # Adicionar a coluna 'hash' ao DataFrame
            df['sk_empregador'] = hashes_empregador

            # Concatenar as colunas do DataFrame
            concatenacao_movimentacao = df['tipomovimentação'] + df['categoria'] + df['cbo2002ocupação'] + df['indtrabintermitente'] + df['indtrabparcial'] + df['indicadoraprendiz'] + df['indicadordeexclusão'] + df['indicadordeforadoprazo']
            # Calcular o hash para a concatenação das colunas
            hashes_movimentacao = concatenacao_movimentacao.apply(lambda x: hashlib.md5(x.encode()).hexdigest())
            # Adicionar a coluna 'hash' ao DataFrame
            df['sk_movimentacao'] = hashes_movimentacao

            # Renomar colunas
            df = df.rename(columns={
                'competênciamov': 'sk_periodo',
                'município': 'sk_localidade',
                'idade': 'nu_idade',
                'seção': 'is_secao_econ',
                'subclasse': 'is_subclasse_econ',
                'saldomovimentação':'nu_saldo',
                'cbo2002ocupação':'is_cbo_ocupacao',
                'categoria':'is_categoria',
                'graudeinstrução':'is_grau_instrucao',
                'horascontratuais':'nu_hora_contratual',
                'raçacor':'is_etnia',
                'sexo':'is_genero',
                'tipoempregador':'is_tp_empregador',
                'tipoestabelecimento':'is_tp_estabelecimento',
                'tipomovimentação':'is_tp_movimentacao',
                'tipodedeficiência':'is_tp_deficiencia',
                'indtrabintermitente':'is_fl_intermitente',
                'indtrabparcial':'is_fl_trab_parcial',
                'salário':'vl_salario',
                'tamestabjan':'is_faixa_tam_estab',
                'indicadoraprendiz':'is_fl_aprendiz',
                'indicadordeexclusão':'is_fl_exclusao',
                'indicadordeforadoprazo':'is_fl_fora_prazo'
                })

            # Excluir coluna não usadas
            df = df.drop([
                'unidadesaláriocódigo',
                'valorsaláriofixo',
                'região',
                'uf',
                'origemdainformação',
                'competênciadec',
                'competênciaexc'
                ], axis=1)

            # Adiciona o dataframe à lista de dataframes
            dfs.append(df)

            try:
                os.remove(f'{data_folder}/{filename}')
            except OSError as e:
                print(f"Error:{ e.strerror}")

    # concatena os dataframes em um único dataframe final
    df_group = pd.concat(dfs, ignore_index=True)

    print("Dataframe único criado com todos os dados.")
    print("--------------------------------------------------------------------------")

    return df_group

def create_dfs(df_group):
    print("--------------------------------------------------------------------------")
    print("Criando dataframes para geração de modelo dimensional star schema...")

    # Criar df_movimentacao
    df_movimentacao =  df_group[[
        'sk_movimentacao','is_tp_movimentacao','is_categoria','is_cbo_ocupacao','is_fl_intermitente',
        'is_fl_trab_parcial','is_fl_aprendiz','is_fl_exclusao','is_fl_fora_prazo'
        ]].drop_duplicates().reset_index(drop=True)
    df_movimentacao.name = 'df_movimentacao'
    print(f"Dataframe {df_movimentacao.name} criado.")

    # Criar df_trabalhador
    df_trabalhador =  df_group[[
        'sk_trabalhador','is_grau_instrucao','is_genero','nu_idade','is_etnia','is_tp_deficiencia'
        ]].drop_duplicates().reset_index(drop=True)
    df_trabalhador.name = 'df_trabalhador'
    print(f"Dataframe {df_trabalhador.name} criado.")

    # Criar df_periodo
    df_periodo = df_group[['sk_periodo','data_mov','nu_ano','nu_mes']].drop_duplicates().reset_index(drop=True)
    df_periodo.name = 'df_periodo'
    print(f"Dataframe {df_periodo.name} criado.")

    # Criar df_empregador
    df_empregador =  df_group[[
        'sk_empregador','is_tp_empregador','is_tp_estabelecimento','is_secao_econ','is_subclasse_econ','is_faixa_tam_estab'
        ]].drop_duplicates().reset_index(drop=True)
    df_empregador.name = 'df_empregador'
    print(f"Dataframe {df_empregador.name} criado.")

    # Criar df_fato_caged
    df_fato_caged = df_group[[
        'sk_empregador',
        'sk_periodo',
        'sk_trabalhador',
        'sk_movimentacao',
        'sk_localidade',
        'vl_salario',
        'nu_hora_contratual',
        'nu_saldo'
        ]]
    df_fato_caged.name = 'df_fato_caged'
    print(f"Dataframe {df_fato_caged.name} criado.")
    print("--------------------------------------------------------------------------")

    return df_movimentacao, df_trabalhador, df_periodo, df_empregador, df_fato_caged

