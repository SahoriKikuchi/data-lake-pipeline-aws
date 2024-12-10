# Data Harvest Lake

Este repositório contém o código-fonte do Trabalho de Conclusão de Curso (TCC) denominado **"Data Harvest Lake"**, que tem como objetivo implementar um Data Lake para o  setor do agronegócio. O projeto faz uso de pipelines ETL com **AWS Glue** e **AWS Lambda**, além de outras ferramentas da AWS

 ## Visão Geral do Projeto

O **Data Harvest Lake** foi desenvolvido para demonstrar uma arquitetura de dados moderna voltada ao agronegócio, permitindo a ingestão, o armazenamento, o processamento e a análise de grandes volumes de informações. O projeto ilustra como pipelines ETL (Extract, Transform, Load) podem ser aplicados para transformar dados brutos em informações valiosas, viabilizando insights estratégicos para a tomada de decisão.

## Arquitetura

A arquitetura do projeto é baseada em serviços de nuvem da AWS, garantindo escalabilidade, disponibilidade e eficiência no processamento:

- **Amazon S3:** Armazenamento de dados brutos e transformados.
- **AWS Glue:** Criação de jobs ETL transformação dos dados.
- **AWS Lambda:** Funções serverless para acionar pipelines de transformação e interagir com outros serviços(API).
- **Amazon Athena:** Consulta interativa e análise dos dados transformados, facilitando a geração de insights.

 ## Fluxo de Dados

1. **Ingestão de Dados:**  
   Dados de café e batata são armazenados inicialmente no Amazon S3.

2. **Transformação (ETL):**  
   - Jobs do AWS Glue extraem os dados brutos do S3.  
   - Os dados são transformados.  
   - Os resultados transformados são armazenados novamente no S3, em formatos otimizados .csv para ser lido pelo crawler.

3. **Processamento Adicional:**  
   Funções AWS Lambda para transformar dados brutos em forma de eventos.

4. **Consulta e Análise:**  
   O Amazon Athena é utilizado para executar queries SQL sobre os dados transformados.  
   Resultados das consultas podem servir de base para dashboards, relatórios e análises avançadas.

 ## Tecnologias Utilizadas

- **AWS S3:** Armazenamento de dados.
- **AWS Glue:** Criação e gerenciamento de pipelines ETL.
- **AWS Lambda:** Execução de funções automação de tarefas.
- **Amazon Athena:** Consulta e análise dos dados.

<!--##Como funciona-->

IFSP São João da Boa Vista