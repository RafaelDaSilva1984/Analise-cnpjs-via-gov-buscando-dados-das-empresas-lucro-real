# Analise-cnpjs-via-gov-buscando-dados-das-empresas-lucro-real

Há 6 dias • Visível a todos, dentro ou fora do LinkedIn

Objetivo Inicial

Obter dados de empresas cadastradas sob o regime tributário Lucro Real, com foco nas seguintes informações:
CNPJ
UF (Estados)
Cidade
E-mail de contato
Esses dados são extraídos e organizados para posterior análise estratégica e visualização.

Ferramentas Utilizadas no Projeto
Python
Desenvolvimento de um script automatizado para:
Ler CNPJs de um arquivo CSV.
Fazer requisições à API pública.
Processar e limpar os dados obtidos.
Exportar os dados prontos para análise.
Power BI
Criação de um dashboard interativo, com:
Mapas e gráficos para análise regional.
Filtros por cidade, estado e outros critérios.

API publica.cnpj.ws/cnpj
Fonte de dados atualizados com:
Informações cadastrais de empresas brasileiras.
Situação cadastral, natureza jurídica, endereço, contato e muito mais.

HTTP
Utilização do protocolo HTTP para:
Enviar requisições GET à API.
Controlar erros e limites de acesso.
Garantir a automação fluida entre Python e a API.

Parte II – Expansão com Segmentação via CNAE
Com o objetivo de enriquecer a análise, a segunda fase do projeto inclui:
CNPJ
UF
Cidade
E-mail de contato
CNAE principal da empresa (atividade econômica)
Link de pesquisa para o CNAE (facilitando acesso a mais detalhes via buscadores como Google)

Essa adição permitirá análises por setor de atividade, filtragens mais refinadas e estratégias mais personalizadas.
