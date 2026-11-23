# Il binario `next` per un CSV

Il client esegue il blocco `ouf.next-step.v1` del risultato. Per `CALL_TOOL` e `POLL` usa esattamente il tool e gli argomenti ricevuti. Per `WAIT_FOR_HUMAN` mostra il link dell’owner e chiama `ouf.wait` con il riferimento ricevuto. La lettura in `urban.object.search` conclude il percorso.

## Decisioni dell’agente

Con `BLOCKED` e `actor=AGENT`, il client considera le `alternatives` nell’ordine indicato e sceglie unicamente una delle azioni elencate. Il messaggio indica la decisione e le letture da cui ricavarla. Ogni argomento fornito dal MCP è vincolante. L’agente compila i soli campi mancanti secondo l’`inputSchema` del tool scelto. Conserva la richiesta completa, inclusa la chiave, per ogni retry dello stesso comando. Una nuova scelta con contenuto diverso usa una nuova offerta del binario.

- La preview fornisce i nomi delle colonne e le evidenze sui valori. Il piano collega ogni compito alle sue colonne. Una discovery riuscita con zero candidati restituisce il controllo a `source.analysis.read`. Fuori dall’analisi l’agente può cercare un termine pubblicato o formulare una nuova discovery. Il MCP conserva l’intento disponibile nei campi compatibili con lo schema.
- L’agente conserva le coppie richiesta/candidato lette da `semantic.discovery.candidates` e gli identificatori ammessi per l’adozione. Una discovery fallita dentro il piano permette di consultare gli altri compiti. La lettura del piano elenca lo stato del lavoro. La ripresa di un compito usa il suo `taskId` e lo stesso budget. Il coordinatore corrente rappresenta le attese riprendibili con `WAITING`. Un `HELD` per esito ambiguo richiede riconciliazione.
- A fine analisi, `MAPPING_DECISION_REQUIRED` permette di adottare un candidato, proporre un termine mancante, preparare la fonte o cercare altre evidenze. `semantic.search` e `semantic.get` consentono di verificare classe, proprietà e riferimenti di pubblicazione. Per `source.onboarding.create` l’agente usa queste letture e la preview per decidere chiave, classificazione dei campi e mapping. L’owner valida il comando. La revisione semantica e la revisione e attivazione della fonte passano dalla THS.
- Il contesto `analysisId` accompagna le letture e i comandi del percorso, anche nel riferimento firmato di attesa. Il MCP consuma questo campo prima dell’invio al Gateway. Per la creazione della fonte usa la `commandKey` come chiave di idempotenza dell’ammissione.

## Attese e finestre

Per i poll ordinari il MCP usa il maggiore tra il minimo dell’owner e una finestra completa di ammissione. Questo copre anche il primo poll dopo una raffica di chiamate che ha già consumato la finestra condivisa. Una risposta di rifiuto prima del dispatch conosce invece il tempo residuo esatto. Il client attende `retryAfterSeconds` e ripete la stessa richiesta alla riapertura indicata da `windowEndsAt`. Ogni tentativo viene nuovamente autorizzato e ammesso.

Per il picker il client presenta `pickerUrl` e segue il poll su `source.file.upload.status`. Il passaggio a un’attesa firmata richiede che Onboarding esponga una scadenza dell’handoff e una versione osservabile anche in `PENDING`. Il TTL privato della ricevuta dopo il caricamento serve alla sua conservazione.

Il limite serializzato del risultato si applica anche ai rifiuti di ammissione. Se la duplicazione nel testo supera il limite, il MCP privilegia il retry completo in `structuredContent`. Se gli argomenti da soli superano il limite della risposta, interviene `TOOL_RESULT_LIMIT`. Per esempio, `source.onboarding.create` ammette 2 MiB di argomenti e 64 KiB di risultato. La ripetizione integrale dei comandi di queste dimensioni richiede una decisione sul contratto o sul limite del risultato.

Il PET `TestNextOnlyCSVToObjects` usa le osservazioni del giro del 23 novembre per upload, attesa, staging, rifiuti di ammissione, primo compito e discovery vuota. I passi successivi sono simulati. Ogni chiamata usa il precedente `next` e ogni decisione compone gli argomenti dalle letture percorse. Le due esecuzioni coprono il percorso ordinario e il recupero dai rifiuti registrati.
