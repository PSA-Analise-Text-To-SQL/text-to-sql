# Estruturação do Projeto

## 1. PROJETO

- **O que será feito? (problema claro + limites do escopo)**
  Será desenvolvido um sistema de **Text-to-SQL** na forma de chat onde o usuário faz pedidos em linguagem natural e recebe os resultados a partir do banco de dados. No escopo inicial, a configuração e conexão com o banco de dados serão feitas exclusivamente via arquivo `.env`. Funcionalidades *extras* previstas incluem: alterar o banco via interface do usuário e um assistente para sugerir como dividir sistemas monolíticos em microsserviços.

- **Como será feito? (linguagem + ferramenta principal + arquitetura básica)**
  O projeto será construído em **Python**, utilizando a **API do Gemini** como motor de processamento de linguagem natural, e conectado a um **Banco de Dados Relacional**. A arquitetura seguirá os princípios de **Desacoplamento**, criando módulos independentes para cada fluxo do sistema.

- **Onde entra o tema? (IA / Qualidade / Paradigma – aplicação concreta)**
  Este projeto aplica diretamente os conceitos de **Design de Software Avançado**, focando na integração de APIs de IA. O núcleo técnico explora a **Engenharia de Prompt** (aplicação de técnicas avançadas para garantir que a IA não sofra "alucinações" e gere queries sintaticamente corretas) e o desafio de injetar metadados do banco na IA de forma eficiente.

- **O que será desenvolvido, analisado ou comparado? O que será feito?**
  Será desenvolvido um fluxo completo de conversão e análise de dados. O sistema traduzirá pedidos naturais (Ex: "Quais são os 3 produtos mais caros?") em queries estruturadas (Ex: `SELECT nome, preco FROM produtos ORDER BY preco DESC LIMIT 3;`) e retornará os dados tabulados e formatados de volta para a interface do chat.

- **Relevância: Que problema real resolve? Há complexidade suficiente para toda a equipe?**
  Resolve o problema da barreira técnica no acesso e extração de dados, permitindo que usuários não-técnicos façam consultas complexas sem saber SQL. A complexidade é alta e perfeitamente dimensionada para um ciclo de desenvolvimento de quatro semanas.

- **Abordagem de Construção (como será desenvolvido sem improviso?)**
  A construção será guiada por uma **Arquitetura Desacoplada**. O sistema será dividido em componentes com responsabilidades únicas. O desenvolvimento usará como base as literaturas sugeridas de arquitetura de agentes, como as técnicas do Google Cloud.

---

## 2. EQUIPE

- **Quem faz o quê?**
  - **Gustavo:** Criação da interface de Chat interativa e processamento da devolução visual das consultas entregues (criação das tabelas de resultados na tela).
  - **Luan:** Definição da Arquitetura Desacoplada e orquestração do fluxo (como a mensagem sai do chat, passa pelas camadas lógicas, e volta do banco).
  - **Andrey:** Lógica de Extração de Metadados do banco relacional (mapeamento de tabelas, colunas, tipos) para alimentar o prompt de forma inteligente e enxuta.
  - **Igor:** Tratamento de Exceções e execução das queries no banco de dados, garantindo que os retornos do Gemini sejam válidos e executados de forma segura.
  - **Thalles:** Integração direta com a API do Gemini e Engenharia de Prompt (criação de instruções base e técnicas anti-alucinação).

- **As atividades são integradas ou fragmentadas/isoladas?**
  As atividades são estruturalmente isoladas (por meio do desacoplamento da arquitetura), mas funcionalmente **integradas**. O módulo de extração (Andrey) precisa estar alinhado com o criador de prompts (Thalles), enquanto a orquestração (Luan/Igor) liga os retornos da IA com o banco e a interface gráfica (Gustavo).

- **Todos estão integrados? Sabem o que fazer? Abordagem de construção em equipe?**
  Sim. A abordagem da equipe seguirá a construção progressiva do pipeline: primeiro garantindo a comunicação do modelo com o `.env` e uma query simples no banco relacional (o MVP primário), para em seguida iterar sobre refatorações e os extras (interface de mudança de banco e sugestões de microsserviço).
