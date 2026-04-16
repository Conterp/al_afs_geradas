# 🧾 AL-AFS-GERADAS-SYNC

Pipeline automatizado para organizar as **AFs Geradas** no Monday.com em uma estrutura de **Access Level**, garantindo que cada liderança visualize apenas as AFs Geradas dos seus respectivos centros de custo.

---

## 🚀 O que ele faz

- Lê AFs da origem e do destino
- Mapeia `cost_center` para o board de destino correto
- Identifica e cria AFs faltantes
- Atualiza status `PAGO` com base nos boards de pagamentos realizados
- Detecta e remove itens duplicados
- Corrige itens em board/grupo errado
- Remove itens sem origem válida
- Limpa `PAGO` indevido (quando não há pagamento correspondente)
- Gera resumo operacional por etapa com duração total

---

## 🧩 Access Level (1 de 4 pipelines)

Este pipeline faz parte do projeto de níveis de acesso no Monday.com.

- Prefixo `AL` = **Access Level**
- O projeto completo é composto por 4 pipelines integrados:
  - **AFs Geradas**
  - **Base RH**
  - **Pagamentos Realizados**
  - **Faturamento**
- Este repositório cobre o fluxo de **AFs Geradas sync**

---

## 🧩 Estrutura (resumida)

```bash
al_afs_geradas/
├── Dockerfile
├── docker-compose.yml
├── requirements.txt
└── src/
    ├── main.py
    ├── config/settings.py
    └── core/monday/
        ├── execute_monday_query.py
        ├── origin/
        ├── destination/
        │   ├── duplicates/
        │   ├── orphans/
        │   └── summary/
        └── payments/
```

---

## ⚙️ Configuração

Crie o `.env` a partir do exemplo e preencha as variáveis obrigatórias:

```env
MONDAY_API_TOKEN=seu_token
MONDAY_BASE_URL=https://api.monday.com/v2
PIPELINE_SHOW_PROGRESS=true
```

> Use sempre `CHAVE=valor` sem aspas e sem espaço após `=`.

---

## 🧪 Execução

### Local
```bash
python -u -m src.main
```

### Docker
```bash
docker compose up --build
```

---

## 🌬️ Airflow (produção)

- `dag_id`: `al_afs_geradas_sync`
- cron: `30 9,11,14,16,17 * * 1-6` (seg-sáb: 09:30, 11:30, 14:30, 16:30, 17:30)

Comando da task:

```bash
docker run --rm \
  --env-file /opt/automations/al_afs_geradas/.env \
  conterp-al-afs-geradas-app:latest
```

---

## 📊 Saída operacional

O pipeline imprime:

- painéis de auditoria (`Wrong board`, `Wrong group`, `No origin`, `Wrong pago`)
- contagem por board e por ação
- resumo final por etapa (`planned`, `success`, `error`)
- duração total da execução

---

## 🔒 Segurança

- Segredos via `.env` (não versionar)
- Execução conteinerizada
- Retry/backoff para chamadas de API
- Recomenda-se rotação periódica do token da API

---

## 🤝 Autor

**João Carser**  
[github.com/JoaoCarser](https://github.com/JoaoCarser)
