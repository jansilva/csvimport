# coding: utf-8
"""
tests
~~~~~~~~~~~~~~~~
Um módulo contendo rotinas especializadas
de importação e alguns tratamentos para arquivos csv.
:copyright: (c) Aqui é free!!!!
:license: pois não, pode passar.
"""

import unittest

from csvimporter import FasciclesCsvHandler, write_csv_header, write_row


class TestCsvImporter(unittest.TestCase):

    def test_in_memory_mode(self):
        """
        Vai testar se a linha de argumento passada ao csvimporter funciona de
        acordo.: in_memory deve setar o atributo interno 'in_memory' do objeto
        para True.
        :return:
        """
        with FasciclesCsvHandler('./natural_keys_scl.csv', True) as m:
            self.assertTrue(m.in_memory)

    def test_in_disk_mode(self):
        """
        Vai testar se a linha de argumento passada ao csvimporter funciona de
        acordo.: in_memory deve setar o atributo interno 'in_memory' do objeto
        para True.
        :return:
        """
        with FasciclesCsvHandler('./natural_keys_scl.csv', False) as m:
            self.assertFalse(m.in_memory)

    def test_empty_path(self):
        """
        Testa uma passagem de path vazio para o arquivo csv. O que vai
        culminar em um ValueError.
        :return:
        """
        with self.assertRaises(ValueError):
            with FasciclesCsvHandler('', True) as m:
                m.process_file()

    def test_not_file_path(self):
        """
        Testa a passagem de um diretório e não um path para um arquivo.
        :return:
        """
        with self.assertRaises(ValueError):
            with FasciclesCsvHandler('./', True) as m:
                m.process_file()

    def test_key_filter_logic(self):
        """
        Esta função testa se dado um csv de entrada, o filtro para chaves
        naturais que geram csv de saída está correto e se seus contadores
        exibem resultado correto para total de registros,
        chaves duplicadas, chaves únicas
        e ocorrências para first page e last page.
        :return:
        """
        with FasciclesCsvHandler('./tests/natural_keys_scl.csv', False) as m:
            m.process_file()
            self.assertEqual(291295, m.reader.num_rows)
            self.assertEqual(285217, m.num_unique_keys)
            self.assertEqual(1852, m.num_dup_keys)
            self.assertEqual(100, m.num_first_or_last)

if __name__ == '__main__':
    unittest.main()
