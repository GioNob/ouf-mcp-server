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
