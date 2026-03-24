## Projeto Text-to-sql

### MVP
Chat onde usuário faz pedidos e retorna o resultado
Para trocar/conectar com o banco precisa acessar o .env
### Extras
Alterar o banco pela interface do usuário
O sistema oferecer sugestão de como dividir o sistema monólito em micro-serviço
### Aspectos Técnicos
Banco: Relacional
API: Gemini
Linguagem: Pyhton
### Desafios
Analise e conversão da IA para o banco.
## Casos de Uso (Exemplos)

### **Caso 1:**

_Usuário:_ "Quais são os 3 produtos mais caros?"

_SQL Gerado:_ `SELECT nome, preco FROM produtos ORDER BY preco DESC LIMIT 3;`

_Consulta Entregue:_

| Nome      | Preco      |
| --------- | ---------- |
| Carro     | R$5.000,00 |
| Moto      | R$2.000,00 |
| Bicicleta | R$500,00   |
### **Caso 2:**

_Usuário:_ "Me mostre os pedidos feitos no mês de janeiro de 2024 que ainda não foram pagos."

_SQL Gerado:_ `SELECT * FROM pedidos WHERE data_pedido BETWEEN '2024-01-01' AND '2024-01-31' AND status = 'pendente';`

_Consulta Entregue:_

| Pedido    | Data       | ... |
| --------- | ---------- | --- |
| Carro     | 01/01/2024 | ... |
| Moto      | 03/01/2024 | ... |
| Bicicleta | 11/01/2024 | ... |
## Relação com a Disciplina

- **Design de Software:** O projeto exige pensar em como "alimentar" a IA com metadados (tabelas, colunas, tipos) de forma eficiente sem estourar o limite de tokens.

- **Engenharia de Prompt:** Aplicação de técnicas avançadas para garantir que a IA não sofra "alucinações" e gere queries sintaticamente corretas.

- **Arquitetura Desacoplada:** Criação de componentes para cada aspecto do projeto como: Extração, Construção de prompts, tratamento de exceção, etc.
## Material de Estudo

https://cloud.google.com/blog/products/databases/techniques-for-improving-text-to-sql
https://arxiv.org/pdf/2501.13594
https://huggingface.co/docs/smolagents/examples/text_to_sql
