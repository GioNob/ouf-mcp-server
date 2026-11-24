# Registro D-065 · ouf.analysis.summary.v2

Il manifest distribuito contiene i default del profilo T0 in `source.analysis.read.operationalLimits`. Il kernel valida gli override all’avvio. Il tetto di risposta comprende il risultato MCP serializzato, con testo, contenuto strutturato, cursore e passo successivo.

| Parametro | Default | Intervallo e unità |
|---|---:|---|
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

## Piano server T2 · analysis-run-v2

Predecessore `9be4dd80e1e7c6a0d1b3f86eda8fb02c4909fc91`, migrazione additiva 018. I tre processi sono il server MCP, `analysis-worker` e `analysis-reservations`. Usano lo stesso PostgreSQL. Il listener delle prenotazioni ha un proprio pool e un proprio endpoint HTTPS. Lo start richiede la delega HUMAN corrente. Il worker usa la credenziale di workload già configurata per il Gateway.

| Parametro di installazione | Uso |
|---|---|
| `OUF_ANALYSIS_RUN_ENABLED` | Booleano stretto. Assente o `false` conserva la preview T1 e produce `ANALYSIS_EXECUTOR_DISABLED` per uno start T2 esplicito. `true` carica il profilo e il manifest. |
| `OUF_ANALYSIS_MANIFEST_FILE` | File UTF-8 immutabile, distribuito byte per byte anche come `manifestFile` al Gateway. Il contenuto JSON deve coincidere con il catalogo compilato. Il mandato usa SHA-256 del file effettivo, inclusi terminatori di riga. |
| `OUF_ANALYSIS_WORKLOAD` | Identità del client di servizio verificata dal Gateway al dispatch. È separata dall'identità del client interattivo memorizzata nello scope. |
| `OUF_ANALYSIS_RUN_LIMITS` | Oggetto JSON completo del profilo seguente. L'installazione conserva gli stessi parametri sulle repliche. Un cambio incompatibile ferma il piano per riconciliazione. |
| `OUF_ANALYSIS_RESERVATION_LISTEN` | Bind privato del comando `analysis-reservations`. Il Gateway invia POST a `/internal/analysis/v1/reservations/check`. |
| `OUF_ANALYSIS_RESERVATION_CERT_FILE`, `OUF_ANALYSIS_RESERVATION_KEY_FILE` | Identità TLS server del listener privato. |
| `OUF_ANALYSIS_GATEWAY_CA_FILE`, `OUF_ANALYSIS_GATEWAY_CLIENT_URI` | Trust store dedicato e URI SAN esatto del certificato client Gateway. Il listener richiede mTLS e TLS 1.3. |

Default del profilo:

```json
{
  "OwnerDedupQualified": false,
  "ProofSeconds": 5,
  "WaitSeconds": 3600,
  "LifetimeSeconds": 86400,
  "PreviewPageSize": 64,
  "ActivePlans": 1,
  "ProviderSlots": 1,
  "Maximum": {"Calls":4096,"Attempts":4096,"Objects":409600,"Bytes":67108864,"Millis":3600000},
  "Shared": {
    "TenantQueue":32,"SourceQueue":4,"TenantSlots":4,"SourceSlots":1,"DependencySlots":4,
    "TenantPerMinute":120,"SourcePerMinute":30,"DependencyPerMinute":120,
    "TenantBytes":1073741824,"SourceBytes":134217728
  }
}
```

`OwnerDedupQualified` registra la decisione d'installazione dopo la qualificazione owner. Il worker richiede entrambi i flag. Il Gateway mantiene il proprio gate indipendente. Il deploy attiva i processi dopo le prove D-090. Per il ritorno, fermare i worker, chiudere il dispatch Gateway e attendere scadenza delle prove concesse e riconciliazione degli effetti in volo.

La lease di verifica dura al massimo `ProofSeconds` (1–30 s) e termina comunque entro la scadenza del tentativo, del mandato e del budget. Il tetto per incidente è 128 tentativi per operazione e intento in una sessione finita. Chiamate e tentativi sono unità intere, byte e millisecondi contano sul piano. Le frequenze condivise usano una finestra mobile di 60 secondi. Il calcolo dei byte condivisi conserva le riserve incerte e addebita anche payload già ridotti a evidenza. Lo scope sorgente usa il riferimento immutabile all'asset. I pool di dipendenza sono Onboarding e Semantic. Le lease dei provider remoti dietro Semantic richiedono la verifica owner.

| Archivio PostgreSQL, schema `ouf_mcp` | Contenuto protetto |
|---|---|
| `analysis_run`, `analysis_run_command` | Scope, profilo e policy fissati, configurazione, mandato, prima chiave e alias, attesa, fence, coda e checkpoint. |
| `analysis_run_attempt`, `analysis_run_request` | Prenotazione, hash, ordinali, worker, prova concessa, limiti e consuntivo, riferimenti di decisione e relazione job–intento. Le prenotazioni con effetto incerto mantengono il massimo originario. |
| `analysis_run_outcome`, `analysis_run_snapshot`, `analysis_run_read` | Versioni degli esiti per ordinale, contatori e audit delle pagine. `STATUS` legge dati locali. |
| `analysis_run_incident`, `analysis_run_budget_session` | Incidente stabile, versione attesa, attore HUMAN, motivo, riferimento alla decisione, limiti e storia delle sessioni. Il punto di confine fra sessioni è una fence. |

La procedura di rinnovo richiede un adattatore di autorità corrente per la decisione esatta. Questo candidato fornisce la transazione e l'interfaccia Go. Il binding di esercizio e il trasporto della revoca verso un MCP in pausa richiedono integrazione con gli owner. Restano da qualificare archiviazione e capacità fisica delle tabelle e degli indici. La migrazione non espurga la storia.

I PET `MCP-50U-01`–`16` coprono file e hash, configurazione, deadline, alias, claim concorrenti, lease, fixture SDK a 18 colonne, crash, rinnovo HUMAN, autenticazione del listener, ritorno 017→018→50t→candidato, pausa/revoca, disattivazione, ripresa, schemi e retry finiti di issue. Le fixture usano riferimenti sintetici. La fixture SDK misura separatamente il percorso pubblico e le chiamate interne; il PET del listener verifica mTLS su loopback.
