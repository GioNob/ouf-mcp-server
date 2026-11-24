# Il binario `next` per un CSV

Il client esegue il blocco `ouf.next-step.v1` del risultato. Per `CALL_TOOL` e `POLL` usa esattamente il tool e gli argomenti ricevuti. Per `WAIT_FOR_HUMAN` mostra il link dell’owner e chiama `ouf.wait` con il riferimento ricevuto. La lettura in `urban.object.search` conclude il percorso.

## Decisioni dell’agente

Con `BLOCKED` e `actor=AGENT`, il client considera le `alternatives` nell’ordine indicato e sceglie unicamente una delle azioni elencate. Il messaggio indica la decisione e le letture da cui ricavarla. L’agente conserva gli argomenti forniti dal MCP, applica le correzioni richieste esplicitamente dall’alternativa e compila i campi mancanti secondo l’`inputSchema` del tool scelto. Conserva la richiesta completa, inclusa la chiave, per ogni retry dello stesso comando. Una nuova scelta con contenuto diverso usa una nuova offerta del binario.

- La preview fornisce i nomi delle colonne e le evidenze sui valori. Il piano collega ogni compito alle sue colonne. Una discovery riuscita con zero candidati restituisce il controllo a `source.analysis.read`. Fuori dall’analisi l’agente può cercare un termine pubblicato o formulare una nuova discovery. Il MCP conserva l’intento disponibile nei campi compatibili con lo schema.
- L’agente conserva le coppie richiesta/candidato lette da `semantic.discovery.candidates` e gli identificatori ammessi per l’adozione. Una discovery fallita dentro il piano permette di consultare gli altri compiti. La lettura del piano elenca lo stato del lavoro. La ripresa di un compito usa il suo `taskId` e lo stesso budget. Il coordinatore corrente rappresenta le attese riprendibili con `WAITING`. Un `HELD` per esito ambiguo richiede riconciliazione.
- A fine analisi, `MAPPING_DECISION_REQUIRED` permette di adottare un candidato, proporre un termine mancante, preparare la fonte o cercare altre evidenze. `semantic.search` e `semantic.get` consentono di verificare classe, proprietà e riferimenti di pubblicazione. Per `source.onboarding.create` l’agente usa queste letture e la preview per decidere chiave, classificazione dei campi e mapping. L’owner valida il comando. La revisione semantica e la revisione e attivazione della fonte passano dalla THS.
- Il contesto `analysisId` accompagna le letture e i comandi del percorso, anche nel riferimento firmato di attesa. Il MCP consuma questo campo prima dell’invio al Gateway. Per la creazione della fonte usa la `commandKey` come chiave di idempotenza dell’ammissione.
- Per una fonte nuova, `create` riceve i riferimenti nella forma `semanticId@semanticVersion`. Se Onboarding segnala `EXECUTION_PROPOSAL_REQUIRED`, l’agente completa la proposta nell’alternativa `source.onboarding.review.prepare`. Recupera i quattro pin da `semantic.get`, usa il set di pubblicazione per `semanticPublicationSetRef`, compila le due politiche UDP secondo lo schema e ricava i limiti dalla preview. La preparazione diretta di una bozza richiede l’attestazione `executionProposalRequired: false`. Dopo `ONB_SEMANTIC_REF_FORM_INVALID`, l’agente corregge il riferimento alla posizione `index` negli argomenti offerti e conserva la nuova chiave per il comando corretto.

## Attese e finestre

Il client mostra il link emesso da Onboarding e chiama subito `ouf.wait`. La risposta invariata a fine tranche riporta `WAIT_FOR_HUMAN` con lo stesso riferimento. Il client può rinnovarlo fino alla scadenza assoluta. Ogni rilettura verifica la delega corrente. Il widget apre il link su gesto dell’utente. In modalità picker, `MCP_MANAGED_FILE_PICKER_URL` fornisce l’origine per la direttiva CSP `redirect_domains` del widget. Il link presentato proviene dalla risposta di emissione.

| Parametro | Variabile MCP | Default | Validazione e uso |
|---|---|---:|---|
| `pollIntervalSeconds` | `OUF_WAIT_INTERVAL_SECONDS` | 5 s | Intero da 5 a 45, inferiore alla tranche. Intervallo minimo tra riletture e minimo del poll macchina. |
| Durata della tranche | `OUF_WAIT_DURATION_SECONDS` | 45 s | Intero da 5 a 45, superiore all’intervallo. Configurare un timeout client di almeno 60 s per la tranche predefinita. |
| `waitMaxSeconds` | `OUF_WAIT_MAX_SECONDS` | 3600 s | Intero da 1 a 3600. Limita il poll macchina a partire da `createdAt` o `startedAt`. Onboarding conserva il proprio termine per le attese del picker. |
| `admissionCallsPerMinute` | `OUF_ADMISSION_CALLS_PER_MINUTE` | 15 | Intero positivo fino a 2147483647. Valori vuoti o fuori intervallo impediscono l’avvio. |

La finestra ordinaria segue il minuto PostgreSQL per principal e tenant. La terza richiesta di comando equivalente riceve un rifiuto. Le letture di stato riconosciute possono usare la capacità residua. Una tranche costa una chiamata ordinaria. Le osservazioni interne impegnano i budget di byte e oggetti. Una prenotazione atomica in PostgreSQL coordina le riletture dello stesso riferimento fra repliche. Restano applicati i limiti di concorrenza `OUF_WAIT_PER_PRINCIPAL`, `OUF_WAIT_MAX_ACTIVE` e di frequenza `OUF_WAIT_OWNER_READS_PER_SECOND`. Per continuare dopo un riavvio le istanze usano la stessa chiave di attesa o la stessa chiave di installazione da cui viene derivata.

Per i lavori di macchina il client segue `POLL` e la cadenza minima dell’owner. La scadenza è il primo termine disponibile fra quelli dell’owner e l’inizio del lavoro più `waitMaxSeconds`. In assenza di entrambi il campo è omesso. Alla scadenza, `WAIT_DEADLINE_EXPIRED` indica come rileggere un esito già registrato. Per un selettore scaduto si può anche aprire un nuovo selettore su richiesta dell’utente. Le letture di handoff legacy richiedono una nuova emissione. Il rifiuto di ammissione prima del dispatch conserva la finestra di ripresa dell’operazione.

Ogni errore espone lo stesso `next` nel testo JSON e in `structuredContent`. Il limite serializzato comprende entrambe le copie. Quando il risultato supera il limite, `TOOL_RESULT_LIMIT` indica il passo di verifica in entrambi i canali. Per esempio, `source.onboarding.create` ammette 2 MiB di argomenti e 64 KiB di risultato.

Il PET `TestNextOnlyCSVToObjects` usa una fixture di emissione e staging conforme al 52a e le osservazioni del giro del 23 novembre per il rifiuto di ammissione, il primo compito e la discovery vuota. I passi successivi sono simulati. Ogni chiamata usa il precedente `next` e ogni decisione compone gli argomenti dalle letture percorse. Le due esecuzioni coprono il percorso ordinario e la ripresa dopo il rifiuto di ammissione della preview.

### Rinnovo del budget di analisi

Su `source.analysis.read` in vista `STATUS`, un incidente di piano esaurito espone `BLOCKED / ANALYSIS_BUDGET_EXHAUSTED`. Per una proposta aperta la prima alternativa è `WAIT_FOR_HUMAN`. La chat presenta `human.reviewUrl` al titolare e chiama `ouf.wait` con il riferimento firmato. L'attesa osserva lo stesso `analysisId` con la vista `STATUS`. Alla scadenza il riferimento smette di consentire la decisione e l'incidente resta aperto.

Dopo `CHANGES_REQUESTED` il passo indica `source.analysis.renewal.propose` con incidente, versione, chiave e `requestedChanges`. Il tool accetta anche un `increment` esplicito con le cinque dimensioni. Senza nuovi valori conserva quelli precedenti per il confronto con il feedback originale. La nuova versione restituisce `previous`, `diff`, incremento e totale, e rimanda a `STATUS` per il collegamento corrente.

L'approvazione riprende il piano con un poll di `STATUS` entro la deadline originaria. Dopo la chiusura, `STATUS` porta a `SUMMARY`. Le pagine conservano gli esiti e le motivazioni per ordinale. Per gli esiti misti la vista riporta `PARTIAL` e il passo conclusivo indica `ANALYSIS_PARTIAL`.
