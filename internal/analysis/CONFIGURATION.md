# Registro D-065 · ouf.analysis.summary.v2

Il manifest distribuito contiene i default del profilo T0 in `source.analysis.read.operationalLimits`. Il kernel valida gli override all’avvio. Il tetto di risposta comprende il risultato MCP serializzato, con testo, contenuto strutturato, cursore e passo successivo.

| Parametro | Default | Intervallo e unità |
|---|---:|---|
| `OUF_ANALYSIS_MAX_COLUMN_REFORMULATIONS` | 7 | 0–7 nuove ricerche per ordinale del profilo e identità, con un intento di 1–3 parole. Zero disabilita la riformulazione. Il consumo si ricostruisce dai task di discovery oltre il primo per ordinale. Le copie dello stesso input al cambio di epoca sono contate una volta. La tabella 018 funge da cache. |
| `OUF_ANALYSIS_PREVIEW_PAGE_SIZE` | 128 | 1–128 colonne richieste per pagina owner |
| `OUF_ANALYSIS_SUMMARY_PAGE_BYTES` | 65536 | 4096–1048576 byte, entro il budget del tool |
| `OUF_ANALYSIS_CURSOR_SECONDS` | 900 | 1–86400 secondi dalla prima pagina, senza rinnovo alla continuazione |

`SUMMARY.limit` è un parametro della richiesta, con default 32 e massimo 128. Il cursore usa la chiave di fingerprint dell’installazione e il dominio `ouf.analysis.summary.v2`. Le repliche che servono lo stesso piano richiedono la stessa chiave. L’obiettivo onboarding viene fissato al primo piano creato con la chiave emessa dal binario. Un piano manuale conserva l’obiettivo di sola analisi. Le letture usano la finestra ordinaria e il limite configurato con `OUF_ADMISSION_CALLS_PER_MINUTE`.

## Archivi PostgreSQL

| Tabella nello schema `ouf_mcp` | Contenuto e ciclo di vita |
|---|---|
| `analysis_preview_progress` | Prefisso contiguo del profilo verificato. Il confronto dell’offset impedisce di aggiungere due volte una pagina. |
| `analysis_column` | Una riga corrente per ordinale. Contiene intestazione del profilo, riferimento al task o job e destinazione del prossimo passo. |
| `analysis_column_version` | Versioni degli esiti indicizzate per analisi, ordinale e sequenza. |
| `analysis_job_completion` | Attestazione immutabile del conteggio finale, legata al job e all’analisi. Vale anche per una successiva risposta che omette il campo. |
| `analysis_counts` | Versioni dei cinque contatori e del totale, obiettivo iniziale e arresto del piano. |
| `analysis_summary_read` | Scope, riferimento della decisione, sequenza e intervallo servito. La prima pagina conserva il passo dello snapshot. Per `STATUS` gli estremi valgono entrambi −1. |

La migrazione 016 aggiunge queste strutture alla 015. Il piano v1 conserva la propria revisione e il vincolo sul checksum del manifest. I nuovi archivi seguono la custodia del piano. La scadenza limita l’accesso allo snapshot tramite cursore. Il dimensionamento e la conservazione fisica dello storico e delle letture vanno qualificati nell’installazione prima del rilascio. La prova di ritorno usa il predecessore `092bcdc2f5cc7e20a41c6c419e3e5466da822448`.

## Misure PET

`TestSummaryPostgresSnapshotAndCost` conta le righe restituite dalle query di lettura e somma i byte dei campi ricevuti da pgx, prima della decodifica. Riporta `STATUS`, prima pagina e pagina 480–511 a 1.000 colonne. Controlla il piano con `EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON)`. Le letture fisiche delle pagine disco dipendono dalla cache di PostgreSQL. La prova SDK misura separatamente la busta MCP e verifica la revoca fra pagine.

## D-065 · profilo T1 `ouf.semantic.published-batch.v1`

I parametri sono in `semantic.search.batch.operationalLimits` nello snapshot versionato del manifest. Il kernel valida il profilo attivo all'avvio. Il piano fissa il checksum dello snapshot e la visibilità della ricerca. Le lingue sono tag ASCII ordinati e univoci dopo la conversione in minuscolo.

| Parametro | Default | Dominio |
|---|---:|---|
| `batchMaxIntents` | 64 | 1–64 intenti distinti |
| `batchMaxRequestBytes` | 65536 | 1–65536 byte per la busta serializzata |
| `batchMaxResponseBytes` | 262144 | 1–262144 byte, entro `hardMaxResultBytes` |
| `limitPerIntent` | 3 | 1–8 hit |
| `preferredLanguages` | `["it"]` | 1–5 tag secondo A.1 |

La composizione rispetta anche la riserva di 100 oggetti per lettura semantica. Con 3 hit ammissibili per intento entrano fino a 33 intenti per lotto, con un solo hit fino a 64. Il consumo conta le revisioni distinte restituite.

Lo snapshot di confronto proviene da Gateway task-51m. La copia in `internal/manifest/testdata/semantic-search-batch-v1.json` conserva la provenienza A.1–A.3 e ha SHA-256 `e28936bd3d40d90adf903091cfe42c86f57570d44578daf30050393adb719edd`.

Il tipo predefinito per un'intestazione è PROPERTY. La versione Unicode è **15.1**. `unicode151/Generate.java` genera `tables.gz` dal JAR ICU4J **75.1**. Include decomposizioni canoniche complete, classi combinanti, coppie componibili, default case folding e White_Space. Per rigenerare con JDK 21 dalla directory `unicode151` usare `java -cp icu4j-75.1.jar Generate.java tables.gz`.

La migrazione 017 registra scope, digest della richiesta iniziale, motivo di compatibilità ed eventuale arresto in `analysis_published_plan`. Il task del lotto conserva intenti, gruppi di ordinali e risposta accettata nello stesso commit dei pin, degli esiti e degli eventuali task figli. Una discovery residua conserva la chiave owner prima del dispatch. Lo stato HELD conserva il riferimento al tentativo per la riconciliazione.

Il passo pubblico T1 consuma la finestra interattiva ordinaria. Il dispatch owner usa la finestra di lettura semantica o discovery già configurata. Un 429/503 esplicitamente transitorio usa il budget persistente del task. Il motivo `PUBLISHED_BATCH_UNAVAILABLE` indica il percorso di compatibilità. Un diniego ferma gli altri dispatch del piano.

Il PET `MCP-50T-05` conta i veri `CallTool` SDK dal piano al riepilogo seguendo `next`. PostgreSQL conserva il piano e un owner sintetico usa le 18 intestazioni di `e2e-20261123.json`. La finestra di ammissione del test rimane aperta per misurare il percorso senza attese. I PET degli errori usano anche l'ammissione PostgreSQL.

La colonna additiva `analysis_column.published_first` e il relativo indice danno precedenza ai lotti aperti anche dopo una ripartizione. I writer v1 usano il default `false`.

| PET | Prova |
|---|---|
| MCP-50T-01 | Confronto ricorsivo dello schema con lo snapshot Gateway 51m e dei vincoli di pubblicazione |
| MCP-50T-02 | Identità norm-v1, digest JCS, lingue, tipo, contesto, visibilità e policy |
| MCP-50T-03 | Composizione per numero e byte, ordinali lontani e confine 1024 |
| MCP-50T-04 | Risposta coerente per tutto il lotto, suddivisione e riduzione del limite |
| MCP-50T-05 | Conteggio delle chiamate SDK sui 18 ordinali del 23/11 |
| MCP-50T-06 | Riepilogo di 1000 ordinali, pin, job condiviso e blocco DATATYPE |
| MCP-50T-07 | Errori A.3, compatibilità, riapertura della connessione e scadenza del budget |
| MCP-50T-08 | Path HTTP, busta effettiva e soglia di trasporto |
| MCP-50T-09 | Migrazione 016 → 017, esecuzione del piano ea4033e e ritorno del predecessore |

### Dinieghi delle letture Semantic

`STALE_POLICY_BUNDLE` e `NO_POLICY_BUNDLE` usano il budget transitorio già previsto per le letture. Lo stesso vale per un diniego di policy con `policyVersion` inferiore alla versione della decisione MCP verificata per l’invio. Il MCP accetta la versione come intero JSON fra 0 e 2⁶³−1. Una versione assente mantiene stabile il diniego di permesso. La ricevuta invalida richiede sempre un operatore. Il checkpoint conserva tentativi e `notBefore`, rispetta `Retry-After` e termina il retry automatico con `RETRY_BUDGET_EXHAUSTED`. Questi casi usano la configurazione esistente.

L’identità del piano include il digest di `BundleID` e `BundleVersion`. Ogni `replanOf` copia gli input persistiti sotto il lock della riga sorgente e crea i task e la ricevuta del comando in una transazione. Restano applicabili i limiti per task, colonne, argomenti e capacità complessiva del piano. La vista storica espone i contatori del piano originale. Per il formato precedente il server verifica l’epoca del lotto dalla decisione di ammissione registrata. I tentativi locali di ripianificazione seguono la retention ordinaria delle ricevute.

Nel nuovo piano i figli derivati restano esclusi dalla vista e dal budget delle chiamate fino alla conferma del lotto padre. La transazione che registra l’esito attiva i figli prodotti dal lotto e scarta gli altri input sospesi nelle sue colonne. Ogni split confermato genera i propri figli quando termina.
