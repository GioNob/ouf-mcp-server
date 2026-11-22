# D-080 · Ritorno al predecessore MCP

Il check GitHub Actions `predecessor-return` prova il ritorno su un solo PostgreSQL 17 effimero. Per qualificare una coppia di immagini serve una run verde riferita al commit candidato e all'immagine effettivamente in esercizio. Il riepilogo registra i commit, la run e l'ID dell'artefatto, lo SHA-256 dello ZIP, gli ID Docker e le versioni dello schema.

## Identità delle immagini

Il predecessore predefinito è `843d22059e03bcd58569fff9960bb46ae552b2b2`, schema 15. Il workflow usa queste variabili di repository.

| Variabile | Valore predefinito o fonte |
|---|---|
| `MCP_PREDECESSOR_COMMIT` | `843d22059e03bcd58569fff9960bb46ae552b2b2` |
| `MCP_PREDECESSOR_RUN` | `44904141061` |
| `MCP_PREDECESSOR_ARTIFACT_ID` | `14707905199` |
| `MCP_PREDECESSOR_ZIP_SHA256` | `bcb6002c844e2e6e3c997f5e3bb10b79f63231ca49d3c1a66de952254b15a3ae` |
| `MCP_PREDECESSOR_IMAGE_ID` | Obbligatorio. Usare la riga `sha256:…` in `image-id.txt` alla radice dello ZIP verificato di `acquisition-mcp-image-44904141061`. |

Il passo **Retain qualified MCP image without target activation** di [`ci.yml`](../.github/workflows/ci.yml) scrive `build/acquisition-release/image-id.txt` con `docker image inspect --format '{{.Id}}' ouf-mcp:ci`. Lo stesso ID compare in `image-config.json` nel campo `.[0].Id`. L'ID da impostare è quello dell'immagine Docker attestata per il rilascio.

Il job verifica i metadati dell'artefatto tramite l'API GitHub con `actions: read`, scarica lo ZIP per ID e ne confronta lo SHA-256 prima di estrarre i file. Verifica `source-commit.txt` e `image-id.txt`, carica `image.tar.gz` e controlla l'ID Docker. Entrambe le istanze del predecessore vengono avviate tramite quell'ID.

Il workflow parte su push, pull request o avvio manuale. Per cambiare predecessore, aggiornare insieme le cinque variabili con i riferimenti del rilascio attestato. Il candidato è costruito con il Dockerfile del commit selezionato, cioè la testa della PR nel caso di `pull_request`.

## Sequenza e condizioni di successo

| Fase | Operazioni e verifiche |
|---|---|
| Predecessore | Esegue `ouf-mcp migrate` sul database vuoto. Il server deve dare 204 a `/health/ready`. Tramite MCP crea un'analisi con una task e avvia una `semantic.discovery.request`. Una query collega il tentativo riuscito al claim, all'evento di audit, alla prenotazione riconciliata e alla finestra con consumo registrato. |
| Candidato | Dopo lo stop del predecessore, esegue il proprio `ouf-mcp migrate` sullo stesso database. Il migratore verifica i checksum esistenti e applica eventuali nuove migrazioni. Il candidato crea una seconda analisi e un comando con chiave distinta. |
| Ritorno | Dopo lo stop del candidato, riparte l'immagine iniziale direttamente in modalità `server`. Superata la readiness, `tools/list` deve esporre gli strumenti attesi e il POST operativo a `/api/internal/v1/mcp/operations/status` deve restituire lo stato MCP atteso. Il predecessore rilegge entrambe le analisi e ripete entrambi i comandi con le chiavi originali. |
| Nuova scrittura | Il predecessore esegue un terzo comando e lo ripete. Ogni retry deve riferirsi al risultato originale e a un tentativo riuscito. Il contatore indipendente dell'owner deve riportare una sola esecuzione per chiave. Le righe di audit, budget, idempotenza e tentativo delle prime due fasi devono conservare i riferimenti osservati prima dello stop. |

L'owner di prova serve soltanto bundle di autorizzazione, anteprime sintetiche e ricevute di discovery. Le scritture in PostgreSQL passano dai binari MCP. Dopo la preparazione delle immagini, i container condividono il loopback di un namespace Docker con `--network none`, senza porte pubblicate. I valori di identità sono fixture pubbliche del test. Il database usa ruoli distinti per migrazione e servizio e viene rimosso a fine job.

## Domande aperte

Per qualificare il ritorno sull'immagine di `843d2205` occorrono `MCP_PREDECESSOR_IMAGE_ID`, la disponibilità dell'artefatto e una run verde. Se la readiness al ritorno fallisce, il job si ferma prima delle chiamate MCP, registra entrambe le versioni dello schema e mostra il log di avvio. Un rifiuto dello schema lascia aperta D-080 per quella coppia.
