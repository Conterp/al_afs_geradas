# 🧾 AL-AFS-GERADAS-SYNC

Pipeline automatizado para sincronizar AFs no Monday.com, com criação de faltantes, atualização de `PAGO`, deduplicação e limpeza de inconsistências.

---

## 🚀 O que ele faz

- Lê AFs da origem e do destino
- Mapeia `cost_center` para board de destino
- Cria AFs faltantes
- Atualiza status `PAGO` com base nos boards de pagamentos
- Remove duplicados
- Corrige itens em board/grupo errado
- Remove itens sem origem
- Limpa `PAGO` indevido
- Gera resumo final com tempo de execução

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
- contagem por board
- resumo final por etapa
- duração total da execução (minutos)

---

## 🔒 Segurança

- Segredos via `.env` (não versionar)
- Execução conteinerizada
- Retry/backoff para chamadas de API

---

## 🤝 Autor

**João Carser**  
[github.com/JoaoCarser](https://github.com/JoaoCarser)
