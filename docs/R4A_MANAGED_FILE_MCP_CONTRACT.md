# R4a — managed file via MCP e picker OUF

Stato attestato al 27 settembre 2026: l'operatore ha riportato rollout
Onboarding/MCP/Gateway in modalità PICKER, upload HUMAN riuscito e Asset ID
`8ec8ae90-808a-4d9e-907c-d56de119e376`. Semantic ha poi pubblicato
la revisione `51706bed-81e4-4306-aca1-70119821727d`. Non è attestato
che questo asset sia arrivato a Ingestion, UDP o search: **R-SMOKE OPEN**.
Questo documento descrive il contratto di prodotto e l'evidenza disponibile;
**questa branch PR #45 conserva ancora l'adapter di fetch diretto `hostfiles`**
e non va mergiata/distribuita come percorso picker. Non certifica che il
codice della branch coincida con l'immagine VPS.
Verificare immagine/revisione live prima di un nuovo rollout.

[Handoff completo PET 1.7](https://github.com/GioNob/ouf-semantic-registry/blob/codex/r4a-smoke-semantic-inventory/docs/handoffs/OUF_HANDOFF_2026-09-27_R4A.md).
Autorità: MCP PET v1.4 §21, Gateway PET v1.5 T25,
Source Onboarding PET v1.6 e Cross-Module Alignment Matrix v1.7.

## Contratto neutrale rispetto al chatbot

`source.file.upload` è la capability/tool di proposta e creazione asincrona
per un file gestito. Il client MCP apre il picker OUF per la selezione del
**file**, senza richiedere che l'utente dichiari CSV. Onboarding rileva e
profila il formato; CSV, XLSX e file GIS seguono le rispettive regole PET;
un formato non supportato produce un errore governato. Il flusso non esige
che l'utente copi l'Asset ID in chat. L'adattatore di handoff può variare
secondo l'host MCP, ma non cambia capability, permessi o owner.

Il browser HUMAN raggiunge la route pubblica OUF tramite Gateway e THS;
MCP invoca le route interne autorizzate tramite Gateway con workload token
e contesto HUMAN delegato firmato. Il Gateway verifica l'identità e lo
scope della capability; Onboarding verifica ricevuta, tenant, owner e
risorsa. Il frontend, MCP e gli altri client usano la stessa capability
governata. Nessuna credenziale MinIO, URL storage o byte/base64 del file
entra negli argomenti o nei risultati visibili al modello. Il picker non
autorizza un'approvazione HUMAN attraverso `tools/call`.

L'URL di una pagina picker e la route di handoff non sono scorciatoie per
pubblicare o attivare una fonte. Non recuperare automaticamente allegati
ChatGPT da host `oaiusercontent.com` o da URL proposti dall'agente.
Il vecchio esperimento con `_meta["openai/fileParams"]`, origini esatte
e spool dell'allegato è conservato nella cronologia Git di questo file;
non è il contratto UX corrente. Un host che non supporta il widget può
richiedere un adattatore equivalente, senza modificare l'owner contract.

## Dall'asset all'ingestione

| Operazione | Owner | Condizione |
| --- | --- | --- |
| Upload `source.file.upload` | Onboarding | HUMAN autorizzato; Gateway streaming bounded; asset e digest governati |
| Profile | Onboarding | Asset owner, scope `ouf.managed-source.file.profile`, job idempotente |
| Preview | Onboarding | Profilo redatto, scope `ouf.managed-source.preview` |
| Create DRAFT | Onboarding | Mapping proposto, riferimenti Semantic fissati, `ouf.managed-source.onboarding.create`; nessuna activation implicita |
| Review/approve/publish | THS e owner del dominio | Challenge esatta, verifica HUMAN e backend owner |
| Ingest | Ingestion | Solo PublishedConfigurationBundle compatibile e ACTIVE |
| Resolve/serve | UDP | Identità canonica, review durevole quando ambigua, search autorizzata |

Per la specifica identità del file, il DRAFT
`managed-cinema-8ec8ae90` è inattivo: la semantica UDP generale è ancora
un gate. La PR UDP #34 gestisce la review per record e rifiuta
`resolution.weighted` non eseguito; issue #35 richiede il motore class-neutral.

## Evidenza di release ancora necessaria

1. Test del picker/handoff con due client compatibili, stesso owner contract,
   scope e decisione. Il comportamento concreto della finestra ChatGPT
   non è assunto come comportamento standard di tutti gli host.
2. Route prodotto: early bytes prima della fine del client; 413 senza
   asset parziale, limiti di risorse, media/checksum/auth negativi,
   idempotenza, owner receipt e rollback snapshot.
3. Dall'asset reale: profilo, mapping semantico approvato, DRAFT approvato
   e ACTIVE, run Ingestion, durable ACK, oggetti UDP e search tramite
   Gateway. Un upload riuscito e una pubblicazione Semantic separata
   non chiudono questo ciclo.
4. Recupero, rete/TLS tra host distinti, retention e audit PET restano
   gate propri. Non presentare un PASS di installer o CI come prova E2E.
