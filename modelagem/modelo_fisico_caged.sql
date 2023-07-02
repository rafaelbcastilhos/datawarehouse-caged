----------------------------------------------------------
------------------ CRIAR BASE DE DADOS -------------------
----------------------------------------------------------

-- DROP DATABASE caged;

-- Database: caged
-- Author: 
CREATE DATABASE caged
    WITH OWNER = dbadmin
        ENCODING = 'UTF8'
        TABLESPACE = pg_default
        CONNECTION LIMIT = -1;

----------------------------------------------------------
--------------------- CRIAR TABELAS ----------------------
----------------------------------------------------------

-- DROPAR TABELAS CASO NECESSÁRIO

-- DROP TABLE public.fato_caged CASCADE;
-- DROP TABLE public.dim_empregador CASCADE;
-- DROP TABLE public.dim_periodo CASCADE;
-- DROP TABLE public.dim_trabalhador CASCADE;
-- DROP TABLE public.dim_localidade CASCADE;
-- DROP TABLE public.dim_movimentacao CASCADE;

-- CRIAR TABELAS

CREATE TABLE public.fato_caged (
    sk_empregador varchar(200) NOT NULL,
    sk_periodo varchar(200) NOT NULL,
    sk_trabalhador varchar(200) NOT NULL,
    sk_movimentacao varchar(200) NOT NULL,
    sk_localidade integer NOT NULL,
    vl_salario numeric NOT NULL,
    nu_hora_contratual integer NOT NULL,
    nu_saldo integer NOT NULL,
    PRIMARY KEY (sk_empregador, sk_periodo, sk_trabalhador, sk_movimentacao, sk_localidade)
);


CREATE TABLE public.dim_empregador (
    sk_empregador varchar(200) NOT NULL,
    is_tp_empregador varchar(20) NOT NULL,
    is_tp_estabelecimento varchar(100) NOT NULL,
    is_secao_econ varchar(100) NOT NULL,
    is_subclasse_econ varchar(200) NOT NULL,
    is_faixa_tam_estab varchar(20) NOT NULL,
    PRIMARY KEY (sk_empregador)
);


CREATE TABLE public.dim_periodo (
    sk_periodo varchar(8) NOT NULL,
    data_mov date NOT NULL,
    nu_ano integer NOT NULL,
    nu_mes integer NOT NULL,
    PRIMARY KEY (sk_periodo)
);


CREATE TABLE public.dim_trabalhador (
    sk_trabalhador varchar(200) NOT NULL,
    is_grau_instrucao varchar(50) NOT NULL,
    is_genero varchar(20) NOT NULL,
    nu_idade integer NOT NULL,
    is_etnia varchar(20) NOT NULL,
    is_tp_deficiencia varchar(20) NOT NULL,
    PRIMARY KEY (sk_trabalhador)
);


CREATE TABLE public.dim_localidade (
    sk_localidade integer NOT NULL,
    is_regiao varchar(20) NOT NULL,
    is_uf varchar(20) NOT NULL,
    is_municipio varchar(50) NOT NULL,
    PRIMARY KEY (sk_localidade)
);


CREATE TABLE public.dim_movimentacao (
    sk_movimentacao varchar(200) NOT NULL,
    is_tp_movimentacao varchar(200) NOT NULL,
    is_categoria varchar(200) NOT NULL,
    is_cbo_ocupacao varchar(200) NOT NULL,
    is_fl_intermitente varchar(20) NOT NULL,
    is_fl_trab_parcial varchar(20) NOT NULL,
    is_fl_aprendiz varchar(20) NOT NULL,
    is_fl_exclusao varchar(20) NOT NULL,
    is_fl_fora_prazo varchar(20) NOT NULL,
    PRIMARY KEY (sk_movimentacao)
);


ALTER TABLE public.fato_caged ADD CONSTRAINT FK_fato_caged__sk_empregador FOREIGN KEY (sk_empregador) REFERENCES public.dim_empregador(sk_empregador);
ALTER TABLE public.fato_caged ADD CONSTRAINT FK_fato_caged__sk_periodo FOREIGN KEY (sk_periodo) REFERENCES public.dim_periodo(sk_periodo);
ALTER TABLE public.fato_caged ADD CONSTRAINT FK_fato_caged__sk_trabalhador FOREIGN KEY (sk_trabalhador) REFERENCES public.dim_trabalhador(sk_trabalhador);
ALTER TABLE public.fato_caged ADD CONSTRAINT FK_fato_caged__sk_movimentacao FOREIGN KEY (sk_movimentacao) REFERENCES public.dim_movimentacao(sk_movimentacao);
ALTER TABLE public.fato_caged ADD CONSTRAINT FK_fato_caged__sk_localidade FOREIGN KEY (sk_localidade) REFERENCES public.dim_localidade(sk_localidade);
