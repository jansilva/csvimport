# coding: utf-8
"""
csvimporter
~~~~~~~~~~~~~~~~
Um módulo contendo rotinas especializadas
de importação e alguns tratamentos para arquivos csv.
:copyright: GPL V2
:license: pois não, pode passar.
"""
import os
import re
import dbm
import csv
import pickle
import logging
from typing import Sequence
from collections import namedtuple
from unicodedata import normalize
from io import TextIOBase


logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
handler = logging.FileHandler('csvimporter.log')
handler.setLevel(logging.INFO)
formatter = logging.Formatter(
    '%(asctime)s - %(name)s - %(levelname)s - %(message)s')
handler.setFormatter(formatter)
logger.addHandler(handler)


def write_csv_header(fp: 'File', fieldnames: [str]) -> None:
    """
    Uma função de uso geral para escrita de campos no header de um CSV.

    Caso fp e/ou fieldnames sejam nulos gera uma ValueError exception.

    ValueError: Informe objetos file e fieldnames válidos.

    :param fp: Um objeto file para ser usado pelo objeto csv.writer.
    :param fieldnames: Uma lista de strings contendo os campos do cabeçalho.
    :return: None
    """
    if not fp and not fieldnames:
        raise ValueError("Informe objetos file e fieldnames válidos.")
    csv_writer = csv.writer(fp)
    csv_writer.writerow(fieldnames)


def write_row(writer: '_csv.writer', row: 'namedtuple') -> None:
    """
    Escreve uma linha no arquivo csv de saída gerenciado pelo writer.
    :param writer: Objeto csv.writer responsável por escrever a linha.
    :param row: namedtuple contento conteúdo da linha a ser persistida.
    :return:
    """
    writer.writerow(list(row))


class CsvReader:
    """
    CsvReader é um extrator de dados de arquivos CSV que agrupa cada linha em
    uma namedtuple.
    A única diferença entre namedtuples e unicórnios é que as
    primeiras existem.
    """

    def __init__(self, fp: TextIOBase, dialect: str="excel"):
        """
        :param fp: objeto file apontando para o arquivo de leitura dos dados.
        :param dialect: o dialeto a ser utilizado pelo csv.reader.
        """
        self._fieldnames = None
        self._builder = None
        self.reader = csv.reader(fp, dialect)
        self.num_rows = 0

    def __iter__(self) -> 'CsvReader':
        return self

    @property
    def fieldnames(self) -> Sequence[str]:
        """
        Retorna os fieldnames do csv.
        :return: Senquence de strings.
        """
        if self._fieldnames is None:
            self._fieldnames = next(self.reader)
        return self._fieldnames

    @property
    def builder(self) -> type:
        """
        Cria namedtuples baseado nos fieldnames e conteúdos dos dados.
        No caso, atentar para a normalização necessária nos atributos usados
        para a namedtuple, pois esta não permite nada diferente de
        alfanuméricos.
        :return: Um tipo, no caso desde uma namedtuple.
        """
        if self._builder is None:
            fields = self.fieldnames
            attrs = ' '.join(
                re.sub(r'[^\w\d]', '',
                    normalize('NFKD', s.lower())
                    .encode('ascii', 'ignore')
                    .decode('ascii')
                ) for s in fields
            )

            self._builder = namedtuple('Data', attrs)
        return self._builder

    def __next__(self) -> 'namedtuple':
        """
        A cada chamada à next(), checamos se a linha não é o cabeçalho ou uma
        linha vazia, e seguimos em frente retornando namedtuples para cada
        linha encontrada.
        :return: uma namedtuple resultante contendo dados da linha corrente.
        """
        builder = self.builder
        row = next(self.reader)
        while row == [] or row == self.fieldnames:
            row = next(self.reader)
        self.num_rows += 1
        return builder(*row)


class FasciclesCsvHandler:
    """
    Um objeto handler para importar dados de fascículos em formato específico
    pedido pela Scielo. Use com o contextmanager:

    >>> with FasciclesCsvHandler('/tmp/algum_csv', in_memory=True) as m:
    ........m.process_file()
    """

    def __init__(self, csv_path: str, in_memory: bool=True):
        """
        :param csv_path: Path para o arquivo csv de entrada.
        :param in_memory: Um modo de execução que permite escolher entre
                          usar um dicionário na memória para filtrar os dados
                          para fins estatísticos. Ou utilizar disco por meio do
                          módulo dbm. Vale mencionar que, para o módulo dbm,
                          foi utilizada a flag nf na abertura do arquivo de
                          banco para que o mesmo opere no modo "fast", fazendo
                          sincronizações em disco com menos periodicidade.
                          Eu acrescentei esta opção para casos onde o csv seja
                          extremamente grande, e precisamos manter o uso da ram
                          o menor possível.
        """
        if not csv_path or os.path.isdir(csv_path):
            raise ValueError('informe um valor válido para o path')
        self.csv_path = csv_path
        self.num_unique_keys = 0
        self.num_dup_keys = 0
        self.num_first_or_last = 0
        self.in_memory = in_memory
        self.reader = None

    def __enter__(self):
        """
        Faz sentido suportar gerenciamento de contexto aqui, pois este objeto
        trabalha com diversos recursos principalmente arquivos, e é
        interessante oferecer uma forma segura de cleanup ao usuário do objeto.
        :return: None
        """
        self.csv_input = open(self.csv_path, 'r', newline='')
        self.first_last_csv = open('without_first_last.csv', 'w', newline='')
        self.single_key_csv = open('single_key.csv', 'w', newline='')
        self.duplicated_key_csv = open('duplicated_key.csv', 'w', newline='')

        self.first_last_writer = csv.writer(self.first_last_csv)
        self.single_key_writer = csv.writer(self.single_key_csv)
        self.duplicated_key_writer = csv.writer(self.duplicated_key_csv)

        if self.in_memory:
            self.cache = {}
        else:
            self.cache = dbm.open('cache', 'nf')
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.csv_input.close()
        self.first_last_csv.close()
        self.single_key_csv.close()
        self.duplicated_key_csv.close()
        self.cache.clear() if self.in_memory else self.cache.close()
        return False

    def _occurrences_in_memory_cache(self, row: 'namedtuple') -> None:
        """
        Conta as ocorrências da chave natural no cache residente em memória,
        incrementando um contador na segunda posição do valor salvo no cache.
        O cache tem o formato:
             {<natural key>: (<row>, contador)}
        :param row: Uma namedtuple representando a linha corrente
                    a ser analisada.
        :return: None
        """
        key = row.chavenatural
        if key not in self.cache:
            self.cache[key] = [row, 1]
        else:
            self.cache[key][1] += 1

    def _occurrences_in_disk_cache(self, row: 'namedtuple') -> None:
        """
        Conta as ocorrências da chave natural no cache residente em memória,
        incrementando um contador na segunda posição do valor salvo no cache.
        O cache tem o formato:
             {<natural key bytes utf-8>: <pickle object>}

        :param row: Uma namedtuple representando a linha corrente
                    a ser analisada.
        :return: None
        """
        key = bytes(row.chavenatural, 'utf-8')
        if key not in self.cache:
            self.cache[key] = pickle.dumps([tuple(row), 1])
        else:
            data = pickle.loads(self.cache[key])
            data[1] += 1
            self.cache[key] = pickle.dumps(data)

    def count_occurrences_and_save_data(self,
                                        row: 'namedtuple',
                                        occurrences: int) -> None:
        """
        Computa o número de ocorrências das chaves naturais de cada linha e
        salva os dados de acordo com os seguintes critérios:

        1) Filtra as linhas do csv de entrada e gera um csv chamado
           without_first_last.csv.

        2) Gera um csv de saída chamado single_key.csv contendo linhas que não
           possuem duplicação de chave natural no documento original.

        3) Gera um csv de saída, chamado duplicated_key.csv, contendo
           documentos que apresentavam duplicação de chave natural no
           documento de entrada.

        Contabiliza as ocorrências para:
        - O total de dados do documento de entrada: self.reader.num_rows
        - Total de linhas com chave natural duplicada: self.num_dup_keys
        - Total de linhas que não tinham chave natural duplicada:
          self.num_unique_keys
        - Total de linhas onde os campos first page ou last page vieram vazios:
          self.num_first_or_last

        :param row: namedtuple contendo os dados da linha.
        :param occurrences: Inteiro representando as ocorrências de linhas com
                            mesma chave natural no csv de entrada.
        :return: None
        """
        if occurrences > 1:
            self.num_dup_keys += 1
            write_row(self.duplicated_key_writer, row)
        elif occurrences == 1:
            self.num_unique_keys += 1
            write_row(self.single_key_writer, row)

        if not row.firstpage or not row.lastpage:
            self.num_first_or_last += 1
            write_row(self.first_last_writer, row)

    def compute_output_files_and_stats(self):
        """
        Método que despacha as linhas inseridas no cache para os métodos de
        análise de estatística e persistência nos arquivos de saída.
        Ao final, exibe as estatísticas no prompt de comando.
        :return: None
        """
        if self.in_memory:
            for row, occurrences in self.cache.values():
                self.count_occurrences_and_save_data(row, occurrences)
        else:
            key = self.cache.firstkey()
            while key:
                data = pickle.loads(self.cache.get(key))
                row = self.reader.builder(*data[0])
                self.count_occurrences_and_save_data(row, data[1])
                key = self.cache.nextkey(key)

        logger.info('Rodando na memória?[{}]'.format(self.in_memory))
        logger.info('Total de registros: {}'.format(self.reader.num_rows))
        logger.info('Total de registros com chave duplicada: {}'
                    .format(self.num_dup_keys))
        logger.info('Total de registros com chave natural única: {}'
                    .format(self.num_unique_keys))
        logger.info('Total de registros sem first page ou sem last page: {}'
                    .format(self.num_first_or_last))

    def process_file(self) -> None:
        """
        Inicia o processamento de arquivos deste objeto, instancia.
        :return: None
        """
        if not self.reader:
            self.reader = CsvReader(self.csv_input)

        # Escrevendo os headers nos arquivos de saída..
        write_csv_header(self.first_last_csv, self.reader.fieldnames)
        write_csv_header(self.duplicated_key_csv, self.reader.fieldnames)
        write_csv_header(self.single_key_csv, self.reader.fieldnames)

        for row in self.reader:
            if self.in_memory:
                self._occurrences_in_memory_cache(row)
            else:
                self._occurrences_in_disk_cache(row)

        self.compute_output_files_and_stats()
