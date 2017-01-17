# coding: utf-8
"""
main module
~~~~~~~~~~~~~~~~
Um módulo contendo rotinas especializadas
de importação e alguns tratamentos para arquivos csv.
:copyright: (c) Aqui é free!!!!
:license: pois não, pode passar.
"""

import argparse

from csvimporter import FasciclesCsvHandler


if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        description="Extrator de CSV para fascículos")
    parser.add_argument('--in_memory',
                        action="store_true",
                        help='Usa cache em RAM.')
    parser.add_argument('--in_disk',
                        action="store_false",
                        help='Usa cache em disco via dbm module.')
    parser.add_argument('--csv_path',
                        help='Path para o arquivo csv de entrada.')
    args = parser.parse_args()

    with FasciclesCsvHandler(args.csv_path, args.in_memory) as m:
        m.process_file()
