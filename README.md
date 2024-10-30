# integracao-dados-ifpb
Repositório destinado ao projeto que investiga a correlação entre dados de qualidade do ar do AQICN e do MapBiomas para estudo de caso na disciplina de Integração de Dados do Mestrado PPGTI - IFPB

## Ferramentas utilizadas:
- VS Code: IDE utilizada para utilizado para executar de forma rápida os códigos e funções em python. Toda limpeza e tratamento inicial dos dados foi realizado nessa ferramenta que permitiu uma rápida visualização dos dataframes criados. Além disto foi utilizada para integrar o código criado em python com a ferramenta airflow através do uso de DAGs (Directed Acyclic Graph).
- Apache Airflow: Orquestrador de pipelines que possibilitou o gerenciamento e a automatização do fluxo de tarefas criado.
- Docker: Ferramenta gerenciadora de containers que permitiu a criação de um ambiente escalável onde o airflow foi executado.
- MiniIO: No contexto do projeto, o MinIO foi integrado ao fluxo de trabalho para permitir o armazenamento de arquivos e dados processados de maneira escalável e acessível, facilitando o gerenciamento e a recuperação de informações durante as tarefas do Airflow.