# Sistema Financeiro de Consultores

Sistema unificado com login por consultor, isolamento de carteira e dashboard para leitura rápida da saúde financeira dos clientes.

## Frontend Angular (moderno)

Foi adicionado um frontend robusto em Angular no diretório `frontend/`, com:

- login separado por consultor
- isolamento de carteira por usuário
- dashboard moderno com visão de risco e ações
- páginas de clientes e títulos com filtros operacionais

Rodar frontend:

```powershell
cd frontend
npm install
npm start
```

Veja detalhes em `frontend/README.md`.

## Backend API para o Angular

O frontend Angular usa API real em `http://localhost:8000/api`.

Rodar API:

```powershell
python -m uvicorn api:app --host 0.0.0.0 --port 8000 --reload
```

## Validação e atualização de dados (PDF + Excel)

Agora o sistema valida formato antes de atualizar dados, tanto para:

- contas a receber (PDF)
- limites de crédito (Excel, aba `Limite de crédito`)

No Excel, o parser agora varre **todas as abas do arquivo** e registra cobertura por aba
(linhas lidas, candidatas, registros, ignoradas e detecção da seção de crédito).

### Atualização via linha de comando (fluxo admin)

Validações aplicadas:

- presença de registros e vendedores
- linhas não interpretadas/ignoradas (com limite configurável)
- status válidos (`A Vencer`, `Vencido`)
- conferência dos 7 consultores esperados
- detecção de duplicidades

Somente validar PDF + Excel:

```powershell
python scripts/import_pdf.py --pdf "C:\Users\Vitor\Downloads\relatorio geral (1).pdf" --excel "Z:\vendedores_clientes_controle limite.xlsx" --validate-only
```

Validar e importar PDF + Excel:

```powershell
python scripts/import_pdf.py --pdf "C:\Users\Vitor\Downloads\relatorio geral (1).pdf" --excel "Z:\vendedores_clientes_controle limite.xlsx" --import-user adm
```

Se a planilha não trouxer todos os 7 consultores na atualização, use:

```powershell
python scripts/import_pdf.py --pdf "C:\Users\Vitor\Downloads\relatorio geral (1).pdf" --excel "Z:\vendedores_clientes_controle limite.xlsx" --no-strict-vendors --import-user adm
```

Importar somente Excel (limite de crédito):

```powershell
python scripts/import_pdf.py --excel "Z:\vendedores_clientes_controle limite.xlsx" --import-user adm
```

## Decisão de arquitetura

Use **um sistema único** com controle de acesso por usuário:

- Cada consultor entra com seu próprio login.
- Cada consultor enxerga apenas sua carteira.
- O perfil `admin` consegue acompanhar todas as carteiras.
- A atualização de base (importação PDF/Excel) é restrita ao `adm/admin`.

### Atualização pela interface Angular

- A tela `Atualizacao` existe dentro do frontend Angular.
- A opção é visível e funcional apenas para o usuário `adm`.
- Consultores e outros usuários não conseguem acessar/importar.

Essa abordagem é mais robusta do que criar um sistema separado por vendedor, porque mantém governança, evita duplicação e centraliza manutenção.

## O que o sistema entrega

- Importação automática de PDF de contas a receber.
- Importação automática de Excel de limite de crédito.
- Extração de campos por título:
  - vendedor
  - cliente
  - situação
  - saldo
  - valor da parcela
  - emissão
  - vencimento
- Dashboard com:
  - saldo total
  - pressão de vencimentos (7 e 30 dias)
  - saldo vencido
  - limite de crédito total, utilizado e disponível
  - cruzamento cliente x exposição x limite para ação comercial
  - diagnóstico por cliente (Saudável/Atenção/Crítico)
  - ação recomendada para cada cliente
- Exportação em PDF no dashboard com:
  - KPIs e painéis da carteira
  - extrato de boletos atrasados e a vencer
  - saldo disponível por cliente (cliente a cliente)
- Exportação em PDF na página **Analise Cliente** com:
  - todos os dashboards da análise
  - organização cliente a cliente para todo o escopo selecionado
- Tabela detalhada dos títulos para consulta operacional.

## Estrutura

```
api.py
app.py
scripts/import_pdf.py
src/auth.py
src/db.py
src/metrics.py
src/pdf_parser.py
```

## Como rodar

1. Criar ambiente virtual e instalar dependências:

```powershell
python -m venv .venv
.venv\Scripts\activate
pip install -r requirements.txt
```

2. Iniciar a API real:

```powershell
python -m uvicorn api:app --host 0.0.0.0 --port 8000 --reload
```

3. Iniciar o frontend Angular (novo terminal):

```powershell
cd frontend
npm install
npm start
```

4. Importar PDF/Excel pela tela **Atualizacao** no usuário `adm`.

Observação: agora a importação pela interface Angular é bloqueada quando `useMockApi=true`.

## Credenciais iniciais

- Admin:
  - `admin / Admin@123`
  - `adm / Admin@123`
- Consultores:
  - `luiz.carlos.abranches / Consultor@123`
  - `alisson.rocha.alencar / Consultor@123`
  - `dustin.akihiro.maeno / Consultor@123`
  - `thiago.gomes / Consultor@123`
  - `gabriel.moura.de.almeida / Consultor@123`
  - `kallebe.gomes.da.silva / Consultor@123`
  - `guilherme.aparecido.vanzella / Consultor@123`

Altere as senhas após implantação.

## Observações importantes

- O importador cria usuários automaticamente com base no nome do vendedor encontrado no PDF.
- O relatório enviado contém **7 vendedores**.
- O banco local é SQLite (`data/consultores.db`).
- O modo padrão do import substitui os registros anteriores; use `--append` para acumular.
