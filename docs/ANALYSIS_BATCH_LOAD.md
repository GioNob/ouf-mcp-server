# Banco di qualifica di carico D-103

Il test `TestBatchLoadQualification`, abilitato dal tag `batchload`, copre il criterio 3 del §8. I profili `small` e `full` creano rispettivamente 30 analisi × 100 colonne con 5 sessioni MCP e 3.000 analisi × 1.000 colonne con 100 sessioni MCP. Ogni sessione usa una propria identità e avvia il carico insieme alle altre. La CI esegue `small` su PostgreSQL 17 e limita la durata del test a 105 secondi, con un limite esterno di 115 secondi dopo la compilazione.

## Esecuzione

Eseguire su Linux con la versione Go pinnata dal repository, come in CI. Per la variante ridotta, usare un database di test con un ruolo che possa creare e rimuovere il database isolato e lanciare il comando dalla radice.

```sh
export MCP_TEST_DATABASE_URL='postgres://mcp:mcp@localhost:5432/mcp_test?sslmode=disable'
export OUF_BATCH_LOAD_PROFILE=small
go test -tags=batchload ./internal/kernel -run '^TestBatchLoadQualification$' -count=1 -timeout=105s -v
```

Per la scala piena usare un **clone di qualifica scrivibile e già popolato** con dati ripuliti, migrabile dal candidato e senza servizi concorrenti. Il banco conserva i dati esistenti, applica le migrazioni, inserisce un tenant sintetico identificato nel rapporto dal UUID del run, aggiunge tre milioni di colonne di prova e aggiorna le statistiche del planner. Predisporre spazio per righe, versioni, indici e WAL. Le quote e le lease MCP nel clone sono condivise fra i tenant, quindi prima del run attendere la scadenza di eventuali pause e lease copiate. Smaltire il clone al termine della qualifica.

```sh
export OUF_BATCH_LOAD_DATABASE_URL='postgres://mcp@localhost:5432/mcp_qualification?sslmode=require'
export OUF_BATCH_LOAD_PROFILE=full
export OUF_BATCH_LOAD_REPORT_DIR=/var/tmp
export OUF_BATCH_LOAD_QUOTAS='{"window_seconds":60,"connections":20,"gateway":{"ouf.semantic.search.batch":200,"ouf.semantic.discovery":200},"semantic":{"ouf.semantic.discovery":20}}'
go test -tags=batchload ./internal/kernel -run '^TestBatchLoadQualification$' -count=1 -timeout=60m -v
```

`OUF_BATCH_LOAD_REPORT_DIR` indica una directory esistente fuori dalla worktree. Per impostazione predefinita viene usata la directory temporanea del sistema. Il test stampa il percorso e produce `report.json` e `report.md`. In CI i due file vengono raccolti come artifact.

Le quote dei simulatori usano finestre fisse indipendenti per capability Gateway. `0` indica una rotta simulata senza limite. Le rotte sono `ouf.semantic.search.batch`, `ouf.semantic.discovery`, `ouf.semantic.discovery.status` e `ouf.semantic.discovery.candidates`. La quota Gateway predefinita è 4 per 60 s in `small`, 200 per 60 s in `full`. In `small` Semantic limita a 2 per finestra la lettura dei candidati per esercitare entrambi gli strati. Questi valori appartengono alla fixture. Il pool parte da cinque connessioni, come il runtime, e si può dimensionare con `connections`.

## Misure e verifiche

- Ogni analisi riceve `STATUS`, una lettura `BATCH`, una decisione `EXCLUDE` per voce e il replay della ricevuta. Il banco inizializza i profili attraverso lo store, prepara un'offerta eleggibile e lascia il resto delle colonne in coda. Sotto carico invoca anche `RunOnce` per ciascuna analisi e distribuisce i lavori sui quattro stadi southbound. Un solo worker effettua dispatch per il tenant sintetico mentre gli utenti MCP procedono in parallelo. Il carico si conclude con eventuale coda residua, registrata nel rapporto.
- Le latenze sono misurate dal client SDK attraverso il trasporto HTTP in processo, in microsecondi. Le righe DB sono la somma dei contatori delle istruzioni PostgreSQL per chiamata. I byte ricevuti e inviati sono misurati sul socket del pool durante le istruzioni tracciate e includono il framing e l'eventuale TLS. `EXPLAIN` misura separatamente le righe visitate e i buffer. Il rapporto indica campioni, p50, p95, p99 e massimo.
- La coda è campionata prima, durante e dopo il carico. La profondità è espressa in colonne pendenti o in esecuzione. L'età è il tempo in millisecondi dalla creazione della singola analisi con lavoro in coda. I 429 sono attribuiti alla sorgente simulata e aggregati per rotta e finestra attiva.
- Il banco usa le query del candidato per `EXPLAIN (ANALYZE, BUFFERS)` sulla prima e sull'ultima offerta e sulle due pagine `SUMMARY`. L'ultima offerta ha soltanto gli ordinali finali eleggibili. Il banco controlla accesso indicizzato e righe visitate, con le normali impostazioni del planner.
- Un `503` con `Retry-After` sospende il dispatch Semantic per tre ore nell'orologio del worker. La prova cancella il contesto del worker in pausa, ricrea worker e pool, controlla il checkpoint, legge `STATUS` durante l'outage e verifica la ripresa entro quota. Un secondo scenario perde una risposta dopo l'effetto owner, riavvia il worker e verifica una sola richiesta logica. Altri due casi verificano il blocco durevole senza chiamata owner dopo la revoca del grant o la scadenza delle 24 ore.

Il rapporto riguarda PostgreSQL e i percorsi MCP del candidato con dipendenze simulate. La qualifica integrata con Gateway, Semantic, THS e client reali richiede le prove di rilascio del D-103.
