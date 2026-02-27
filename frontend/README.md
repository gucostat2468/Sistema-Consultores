# Frontend Angular - DronePro Consultores

Frontend moderno em Angular para analise financeira por consultor, com isolamento de carteira por login.

## Rodar local

```bash
npm install
npm start
```

Aplicacao: `http://localhost:4200`

## Build

```bash
npm run build
```

## Login por consultor

- `admin` (perfil administrador) -> senha `Admin@123`
- `adm` (perfil administrador com permissão de importação) -> senha `Admin@123`
- `luiz.carlos.abranches` -> senha `Consultor@123`
- `alisson.rocha.alencar` -> senha `Consultor@123`
- `dustin.akihiro.maeno` -> senha `Consultor@123`
- `thiago.gomes` -> senha `Consultor@123`
- `gabriel.moura.de.almeida` -> senha `Consultor@123`
- `kallebe.gomes.da.silva` -> senha `Consultor@123`
- `guilherme.aparecido.vanzella` -> senha `Consultor@123`

## Importação de PDF/Excel na interface

- A opção **Atualizacao** aparece apenas para o usuário `adm`.
- Somente `adm` consegue acessar a rota e executar importação.
- Consultores não visualizam essa opção e não conseguem importar.
- No backend real, a planilha Excel é lida com varredura completa de abas e validação por aba.

## Exportação PDF da carteira

- Na tela **Dashboard**, há o botão **Exportar PDF completo**.
- O relatório inclui KPIs, painéis da carteira e extrato cliente a cliente.
- Para cada cliente, o PDF separa boletos atrasados, boletos a vencer e saldo disponível.

## Exportação PDF da página Analise Cliente

- Na tela **Analise Cliente**, há o botão **Exportar PDF completo (todos os clientes)**.
- O PDF sai organizado cliente a cliente para todo o escopo carregado.
- Inclui todos os blocos da análise: composição, concentração, matriz de risco e top títulos.

## Integracao com backend real

No ambiente de desenvolvimento, o projeto esta com `useMockApi: false` em `src/environments/environment.ts`.

Para rodar com API real:

1. Suba a API:
   - `python -m uvicorn api:app --host 0.0.0.0 --port 8000 --reload`
2. Garanta os endpoints:
   - `POST /auth/login`
   - `GET /consultants`
   - `GET /dashboard/summary`
   - `GET /dashboard/client-health`
   - `GET /dashboard/receivables`
   - `POST /admin/import`
