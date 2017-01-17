Importador de dados CSV para Fascículos.

Bem, o processo de criação consistiu em criar mecanismos para leitura do csv de maneira clara e concisa. E o mais importante,
de maneira segura, fechando descritores e desacolando recursos. Tomei cuidado também para dar opção de uso de cache em RAM e em disco para a filtragem de chaves duplicadas, bem como forneci uma interface de comandos amigável para o uso do programa.


Abaixo, segue a especificação para uso do sistema:

1 - Para filtrar chaves em memória, basta executar: python main.py --in_memory --csv_path ./natural_keys_scl.csv
2 - Para filtrar chaves em disco(com pequeno cache em RAM), basta executar: python main.py --in_disk --csv_path ./natural_keys_scl.csv
3 - Para executar os testes: python -m unittest tests/tests.py


As saídas geradas são:

1 - Um arquivo de log contendo informações de execução solicitadas no desafio: csvimporter/csvimporter.log
2 - Arquivo CSV de saída para linhas que tiveram chaves naturais duplicadas: csvimporter/duplicated_key.csv
3 - Arquivo CSV de saída para linhas que tiveram uma única ocorrência de chaves naturais: csvimporter/single_key.csv
4 - Arquivo CSV de saída para linhas que não possuiam first page ou last page: csvimporter/without_first_last.csv
5 - Arquivo de cache usado pelo módulo dbm: csvimporter/cache


Mais informações foram documentadas no código.
