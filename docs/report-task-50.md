# Report sprint-03 / task-50 — MCP, blocco `next` e attesa delle decisioni

## Esito

`PARTIAL`. Il candidato contiene il contratto, le regole nel manifest, il percorso comune dei risultati MCP e `ouf.wait`. La catena automatica CSV → UDP ha ancora interruzioni documentate sotto. Manca una build verificata del candidato.

## Sintesi

Il manifest `1.15.0-next-step-v1` associa tutte le 35 capability eleggibili a una policy e comprende le regole comuni per i rifiuti. Entrambi entrano nel checksum canonico. Il kernel valida gli argomenti prima del dispatch, conserva il contenuto precedente e aggiunge il passo al risultato strutturato. Ogni destinazione automatica passa per lo schema pubblicato nel modo di installazione attivo.

`ouf.wait` usa la lettura governata associata alla decisione. Supporta i validatori di Onboarding e Semantic e restituisce `changed`, `observation` e `next`. Quando il run ha raggiunto `runState=COMPLETED` e l’owner comunica il tipo, il passo porta a `urban.object.search`.

## Branch e commit

- Branch `sprint-03/task-50-next-step`.
- HEAD `aeebd63e78224f550bf57ea2685d40cb6e0183e6`.
- Commit **modifiche non committate**. L’indice è vuoto rispetto a HEAD.

## File modificati

| File | Tipo | Descrizione |
|---|---|---|
| `contracts/ouf-next-step-v1.json`, `contracts/.gitattributes` | A | Contratto dell’handoff e preservazione dei byte al checkout |
| `internal/manifest/capabilities.json`, `internal/manifest/manifest.go` | M | Policy versionate, registrazione di wait, schemi condizionali, istruzioni |
| `internal/manifest/next.go`, `internal/manifest/next_test.go` | A | Regole, derivazione delle forme ammesse, prove su descrizioni, contratto e checksum |
| `internal/manifest/manifest_test.go` | M | Catalogo atteso e fixture del manifest |
| `internal/nextstep/engine.go`, `internal/nextstep/waitref.go` | A | Proiezione degli stati, classificazione di errori, riferimenti autenticati e vincolo alla risorsa |
| `internal/nextstep/engine_test.go`, `internal/nextstep/rules_test.go`, `internal/nextstep/testdata/transitions.json` | A | Fixture delle transizioni, vocabolari owner, test di catena e dei parametri di destinazione |
| `internal/kernel/workflow.go`, `internal/kernel/wait.go` | A | Registrazione MCP, controllo della dimensione finale, autorizzazione delle riletture, coalescenza e limiti |
| `internal/kernel/kernel.go`, `internal/kernel/acquisition_denial.go` | M | Entry point, istruzioni, diagnostica di rete e degli owner |
| `internal/kernel/acquisition_read.go`, `internal/kernel/profile_analysis.go`, `internal/kernel/source_revision.go`, `internal/kernel/value_types.go` | M | Integrazione dei gestori specializzati con la validazione comune |
| `internal/kernel/kernel_test.go` | M | Discovery del nuovo tool |
| `internal/kernel/workflow_test.go`, `internal/kernel/wait_test.go`, `internal/kernel/wait_clock_test.go` | A | Test SDK su catalogo ed errori, catena dalla revisione a UDP, revoca, concorrenza, tempo massimo e scadenza |
| `docs/report-task-50.md` | A | Report e parametri di installazione |

## Comandi eseguiti

| Comando o verifica | Esito |
|---|---|
| `fc.exe /b` e `Get-FileHash` sulle due copie | Identiche. SHA-256 `1ae7b9a40087feb6af68383eda0c80ac42f4407e99808445abb1b0b49d07926e` |
| PowerShell 7 `Test-Json -Schema` | **46/46** proiezioni automatiche del manifest e **16/16** chiamate con argomenti attesi nelle fixture hanno superato la verifica dello schema di destinazione. Per l’attesa questo controllo include lo schema della lettura sottostante |
| Confronto degli schemi preview di HEAD e candidato | **256/256** combinazioni di selettori hanno conservato il medesimo esito di validazione |
| `python scripts/generate_value_type_schemas.py --check` | Superato |
| `node scripts/test_picker_portability.mjs` | Superato per MCP Apps e ChatGPT |
| `git diff --check` | Superato |
| `go test ./...`, `gofmt -l cmd internal` | Comandi indisponibili nell’ambiente Windows. La toolchain richiesta dal repository è Go 1.27.4 |
| `docker image ls` | Accesso negato al daemon Docker locale |
| CI, `wide-contract`, `predecessor-return` | Nessuna esecuzione sul candidato. La verifica offline degli schemi riguarda i dati di test. La compilazione e l’esecuzione degli handler Go restano da verificare |

## Criteri di accettazione

- [x] Contratto versionato identico all’handoff e validazione offline delle proiezioni di argomenti automatiche.
- [x] `source.file.preview` mantiene la selezione per coppia asset/profilo o asset/job e le viste `ASSETS` e `COLUMNS`. Le forme pubblicate derivano dai quattro rami di `oneOf`, ora espliciti anche per i client che leggono soltanto il primo livello.
- [x] Istruzione D-085 aggiunta al server e alle descrizioni dei tool.
- [x] Parametri di attesa configurabili, URL di revisione forniti dall’owner e tracciabilità D-082 riportata sotto.
- [ ] Tutti gli stati e i risultati esercitati attraverso l’SDK. I test Go del candidato includono catalogo, regole, rifiuti e attesa. Mancano gli esiti esecutivi e la qualificazione congiunta dei quattro owner.
- [ ] Catena autonoma completa. Le prove aggiunte seguono `next` dal caricamento alla selezione semantica e dalla preparazione della revisione all’attesa e alla lettura UDP. Il caso «Chiedi modifiche» indica `source.onboarding.revision.prepare` come alternativa e richiede i dati della nuova revisione.
- [ ] `NO_APPLICABLE_GRANT` trasformato in proposta eseguibile o attesa THS in ogni caso. Sul diniego di pre-ammissione il kernel indica `authorization.permissions.propose`; per acquisition espone la capability e i vincoli richiesti. La validità temporale, il destinatario e l’ID del grant rimangono da decidere.
- [ ] `go test ./...`, formattazione, CI, `predecessor-return` e `wide-contract` verdi sul candidato.

## Decisioni prese

Ho mantenuto lo schema di preview perché l’owner espone due letture distinte per job e profilo. Gli identificativi si prendono dalla risposta e, quando l’owner omette un selettore già fornito, dagli argomenti della stessa invocazione. Una risposta con più run o una pagina parziale richiede una selezione esplicita. Il piano iniziale per un CSV usa i nomi delle colonne per una discovery di proprietà con lingua preferita italiana, entro il limite del batch del coordinatore. I valori di esempio del CSV restano fuori dagli argomenti della discovery.

Il risultato testuale precedente conserva la sua forma per i client che leggono `content[0].text`. Il kernel misura il JSON completo prima di consegnarlo. Se l’aggiunta supera il tetto, restituisce `TOOL_RESULT_LIMIT` e indica la lettura di riconciliazione.

Il riferimento di attesa usa AES-256-GCM. Autentica selettori, capability, versione e scadenza e lega il contenuto a principal, tenant, servizio, emittente, audience e tipo di attore. La chiave predefinita deriva dalla chiave di fingerprint già configurata mediante HMAC-SHA-256 con etichetta dedicata. Le repliche che devono accettare lo stesso riferimento devono condividere la configurazione della chiave.

| Parametro | Default | Vincolo |
|---|---:|---|
| `OUF_WAIT_KEY_BASE64` | Derivazione dalla chiave di fingerprint | Override facoltativo di 32 byte in Base64 standard |
| `OUF_WAIT_INTERVAL_SECONDS` | 5 | Da 5 a 45, inferiore alla durata |
| `OUF_WAIT_DURATION_SECONDS` | 45 | Da 5 a 45, superiore all’intervallo |
| `OUF_WAIT_PER_PRINCIPAL` | 2 | Da 1 a 100 attese concorrenti per identità |
| `OUF_WAIT_MAX_ACTIVE` | 100 | Da 1 a 100 attese concorrenti per processo |
| `OUF_WAIT_OWNER_READS_PER_SECOND` | 20 | Da 1 a 20 per processo, burst di una lettura |

Il limite di 20 letture/s è applicato prima del dispatch. Le richieste contemporanee identiche possono condividere una rilettura quando coincidono anche la delega e il contesto di autorizzazione. Con 100 attese, ogni ciclo individuale dista almeno 5 s dal precedente. Il rate limiter protegge anche il primo ciclo. Per un tetto d’installazione con più processi occorre ripartire tra le repliche il valore di `OUF_WAIT_OWNER_READS_PER_SECOND`. Il numero riportato è un limite implementato per processo; manca una misura sotto carico sul candidato.

## Domande aperte

1. **Continuazione dei comandi.** Quale capability emette la chiave del *nuovo* comando e conserva le decisioni di mapping fra discovery, Semantic e Onboarding? Le ricevute osservate forniscono gli identificativi di revisione, ma alcuni passaggi richiedono una chiave nuova, una selezione semantica, i campi della fonte e i pin pubblicati. Per il retry dello stesso comando il candidato restituisce gli argomenti originali, inclusa la stessa `commandKey`.
2. **Run fermo.** Quale campo effettivamente emesso da Ingestion stabilisce la scadenza della decisione? Il task 50c espone `reviewUrl`, `runId` ed `expectedVersion`. Quando manca la scadenza, il risultato propone la pagina umana e la rilettura. La fixture con scadenza copre `RESUME_OR_ABORT`.
3. **Attivazione.** Il task 50b distingue `HUMAN_ACTIVATE` da `HUMAN_RECHECK`. Il contratto v1 ha un valore per il ricontrollo. Per l’attivazione il candidato informa l’umano con `BLOCKED` e conserva il link nell’osservazione. Quale `decisionKind` dovrà rappresentare l’attivazione?
4. **Permessi.** Dove recuperare gli estremi di una proposta o una challenge di autorizzazione già aperta a partire da un diniego locale? `UPSERT` richiede un grant identificato e una finestra di validità. Una proposta già esistente con URL, versione e scadenza può produrre `WAIT_FOR_HUMAN` con `GRANT_PERMISSION`.

## Note per il reviewer

**Compatibilità D-080.** Il candidato usa i percorsi di persistenza esistenti per le normali capability. Le regole aggiungono metadati in risposta e il client conserva il token di attesa. La modifica non introduce uno schema DB. I piani di analisi restano vincolati al checksum del manifest con il comportamento di rifiuto già previsto dallo scenario `predecessor-return`. I riferimenti di attesa del candidato valgono sulle istanze che pubblicano `ouf.wait`.

**Trasporto.** Il binario imposta una write deadline HTTP di 15 s. Il kernel estende la scadenza per la singola attesa autorizzata quando il writer lo permette. Con il wrapper di metriche che non espone il controller, usa finestre fino a 10 s. In questa configurazione l’intervallo deve lasciare spazio alla rilettura entro la finestra effettiva. Il validatore di Onboarding è `ifObservationVersion`; quello di Semantic è `expectedVersion`. Una ricevuta del predecessore priva del validatore usa la lettura completa. Il transito delle nuove forme attraverso il candidato Gateway richiede la prova pairwise 50f.

| Riferimento D-082 | Test del candidato | Stato dell’evidenza |
|---|---|---|
| `PET-mcp-058 / E2E-X01`, `PET-mcp-070 / E2E-X13` | `TestOwnerTransitionsAndTargetArguments`, `TestNextOnlyFromReviewToObjectSearch` | Fixture e test della catena con owner simulato. L’intero giro verticale resta aperto |
| `PET-mcp-068 / E2E-X11`, `PET-mcp-115` | `TestWaitAdmissionBoundsConcurrentCalls`, `TestWaitRateFor100DistinctReaders`, `TestWaitUnchangedCoalescesAndExpires`, `TestWaitReauthorizesAfterRevocation` | Concorrenza, limite di dispatch per 100 lettori, coalescenza, scadenza e ricontrollo dell’autorità. Test Go da eseguire |
| `PET-mcp-144`, `PET-mcp-168` | `TestEveryAdvertisedToolReturnsGovernedValidation`, `TestEveryAutomaticRuleHasSchemaValidArguments`, `TestOwnerTransitionsAndTargetArguments` | Rifiuti e retry guidati. Le proiezioni di argomenti sono state verificate offline |

