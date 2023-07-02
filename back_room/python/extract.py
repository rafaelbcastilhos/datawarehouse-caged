# importar bibliotecas
import os
from multiprocessing.resource_sharer import stop
from urllib import request
from pyunpack import Archive

def download_files(proximo_ano,proximo_mes,data_folder):
    # Verifica se existe o diretório para os dados, se não existe cria ele
    if not os.path.exists(data_folder):
        os.makedirs(data_folder)

    # Declaração das variáveis próximo ano e próximo mês
    ano_baixar = str(proximo_ano).zfill(4)
    mes_baixar = str(proximo_mes).zfill(2)

    print('-------------------------------------------------')
    print(f'Proximo Mes/Ano a carregar: {mes_baixar}/{ano_baixar}')
    print('-------------------------------------------------')

    # Verifica se o arquivo já foi baixado
    if(os.path.exists(f'{data_folder}/CAGEDMOV{ano_baixar}{mes_baixar}.txt')):
        print(f'Arquivo CAGEDMOV{ano_baixar}{mes_baixar} já foi baixado baixado')

    else:
    # Tenta baixar o primeiro arquivo
        try:
            print(f'Baixando o arquivo CAGEDMOV{ano_baixar}{mes_baixar}.7z')
            request.urlretrieve(f'ftp://ftp.mtps.gov.br/pdet/microdados/NOVO%20CAGED/{ano_baixar}/{ano_baixar}{mes_baixar}/CAGEDMOV{ano_baixar}{mes_baixar}.7z', f'{data_folder}/CAGEDMOV{ano_baixar}{mes_baixar}.7z')
        except:
            print(f'Arquivos de {ano_baixar}{mes_baixar} ainda não disponibilizado')
            print('-------------------------------------------------')
            print('Todos os arquivos disponíveis foram baixados!')
            print('-------------------------------------------------')
            return False

        # Verifica se o arquivo foi baixado
        if(os.path.exists(f'{data_folder}/CAGEDMOV{ano_baixar}{mes_baixar}.7z')):
            print(f'Arquivo CAGEDMOV{ano_baixar}{mes_baixar} baixado')
            
            # Descompacta o arquivo
            print(f'Descompactando o arquivo CAGEDMOV{ano_baixar}{mes_baixar}.7z')
            Archive(f'{data_folder}/CAGEDMOV{ano_baixar}{mes_baixar}.7z').extractall(f'{data_folder}')
            print('-------------------------------------------------')
            
            # Remove o arquivo compactado
            try:
                os.remove(f'{data_folder}/CAGEDMOV{ano_baixar}{mes_baixar}.7z')
            except OSError as e:
                print(f"Error:{ e.strerror}")

    # Verifica se o arquivo foi baixado
    if(os.path.exists(f'{data_folder}/CAGEDFOR{ano_baixar}{mes_baixar}.txt')):
        print(f'Arquivo CAGEDFOR{ano_baixar}{mes_baixar} já foi baixado baixado')

    else:
    # Tenta baixar o segundo arquivo
        try:
            print(f'Baixando o arquivo CAGEDFOR{ano_baixar}{mes_baixar}.7z')
            request.urlretrieve(f'ftp://ftp.mtps.gov.br/pdet/microdados/NOVO%20CAGED/{ano_baixar}/{ano_baixar}{mes_baixar}/CAGEDFOR{ano_baixar}{mes_baixar}.7z', f'{data_folder}/CAGEDFOR{ano_baixar}{mes_baixar}.7z')
        except:
            print(f'Arquivo CAGEDFOR{ano_baixar}{mes_baixar}.7z ainda não disponibilizado')

        # Verifica se o arquivo foi baixado
        if(os.path.exists(f'{data_folder}/CAGEDFOR{ano_baixar}{mes_baixar}.7z')):
            print(f'Arquivo CAGEDFOR{ano_baixar}{mes_baixar} baixado')

            # Descompacta o arquivo
            print(f'Descompactando o arquivo CAGEDFOR{ano_baixar}{mes_baixar}.7z')
            Archive(f'{data_folder}/CAGEDFOR{ano_baixar}{mes_baixar}.7z').extractall(f'{data_folder}')
            print('-------------------------------------------------')

            # Remove o arquivo compactado
            try:
                os.remove(f'{data_folder}/CAGEDFOR{ano_baixar}{mes_baixar}.7z')
            except OSError as e:
                print(f"Error:{ e.strerror}")

    # Verifica se o arquivo foi baixado
    if(os.path.exists(f'{data_folder}/CAGEDEXC{ano_baixar}{mes_baixar}.txt')):
        print(f'Arquivo CAGEDEXC{ano_baixar}{mes_baixar} já foi baixado baixado')
    
    else:
        # Tenta baixar o terceiro arquivo
        try:
            print(f'Baixando o arquivo CAGEDEXC{ano_baixar}{mes_baixar}.7z')
            request.urlretrieve(f'ftp://ftp.mtps.gov.br/pdet/microdados/NOVO%20CAGED/{ano_baixar}/{ano_baixar}{mes_baixar}/CAGEDEXC{ano_baixar}{mes_baixar}.7z', f'{data_folder}/CAGEDEXC{ano_baixar}{mes_baixar}.7z')
        except:
            print(f'Arquivo CAGEDEXC{ano_baixar}{mes_baixar}.7z ainda não disponibilizado')

        # Verifica se o arquivo foi baixado
        if(os.path.exists(f'{data_folder}/CAGEDEXC{ano_baixar}{mes_baixar}.7z')):
            print(f'Arquivo CAGEDEXC{ano_baixar}{mes_baixar} baixado')

            # Descompacta o arquivo
            print(f'Descompactando o arquivo CAGEDEXC{ano_baixar}{mes_baixar}.7z')
            Archive(f'{data_folder}/CAGEDEXC{ano_baixar}{mes_baixar}.7z').extractall(f'{data_folder}')

            # Remove o arquivo compactado
            try:
                os.remove(f'{data_folder}/CAGEDEXC{ano_baixar}{mes_baixar}.7z')
            except OSError as e:
                print(f"Error:{ e.strerror}")

    return True
