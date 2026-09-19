# Stato MCP con profilo PUBLIC_OPERATIONAL

## Contratto e confini

`ouf.system.status` resta un tool senza parametri. Il suo profilo di risposta
è fisso: `PUBLIC_OPERATIONAL`. Prima dell'autorizzazione, l'orchestratore imposta
`resource.attributes.detailLevel=PUBLIC_OPERATIONAL`, senza modificare la mappa
del chiamante. Richieste di un dettaglio diverso sono negate. La decisione deve
consentire questo dettaglio, il tenant corrente e il resource type `capability`,
con un decision reference non vuoto. Negazione, scope mancante e grant scaduto
non producono un tentativo di esecuzione.

La chiamata continua attraverso admission, Gateway, API owner MCP, reconcile e
audit. Nessuna chiamata diretta al datastore dal tool. Il wire contract Gateway
rimane invariato: il riferimento della decisione viaggia nel contesto esistente.
Non viene introdotto un header di dettaglio controllabile dal chiamante.

L'owner richiede il contesto Gateway verificato, tenant, principal e decision
reference. Il riferimento è un prerequisito del contesto attendibile, non una
firma verificata autonomamente dall'owner: rimane necessaria l'isolazione della
rete backend e la validazione del contesto da parte del Gateway. Per questo
endpoint l'owner applica sempre il profilo pubblico anche su accesso API; nessun
header può aumentarlo. L'orchestratore ripete la proiezione sulla risposta del
Gateway per contenere anche un producer precedente o configurato male.

| Campo restituito | Semantica |
| --- | --- |
| module | MCP; nessuna dichiarazione di copertura di tutti i moduli |
| status | HEALTHY, RECOVERING o DEGRADED, conservato dal producer |
| actionRequired | Indicatore aggregato del producer |
| partial | Incompletezza dichiarata dal producer, conservata |
| visibilityClass | PUBLIC_OPERATIONAL |
| redacted | true; dettagli deliberatamente esclusi dal profilo |

L'allowlist elimina contatori, riferimenti agli incidenti, securityIncidentCount,
evidence e campi futuri sconosciuti. Dati obbligatori assenti o malformati
producono indisponibilità, mai uno stato sano inventato. Errori upstream sono
ridotti a NOT_AUTHORIZED oppure STATUS_UNAVAILABLE, senza corpo o dettagli
arbitrari. I limiti dimensionali valgono prima e dopo la proiezione.

Lo store conserva il dato originario per gli altri percorsi autorizzati. La
classificazione della capability sorgente non viene abbassata. Summary,
incidents e livelli superiori non sono implementati da questa correzione.
L'audit riconciliato persiste decision reference, permitted detail e redacted,
senza includere il payload operativo.

## PET e verifiche

Riferimenti consultati: PET Authorization §§34.1–34.3 (scope, dettaglio, redazione
e distinzione tra negazione e assenza incidenti); PET MCP §§33–34 (stato aggregato,
API owner channel-neutral e percorso MCP→Gateway→MCP).

Le regressioni coprono allowlist/idempotenza, input incompleto, decisione con
scope/tenant/dettaglio errati, assenza di dispatch dopo negazione, audit e
riconciliazione, errori upstream e impossibilità di aumentare il dettaglio con
un header. La prova SDK usa il vero evaluator/cache, orchestratore, client HTTP
e owner API; un fixture trasporto simula la mediazione Gateway. Non sostituisce
il collaudo APISIX reale né le prove pairwise contro i repository fissati in CI.

## Installazione e accettazione

Questa modifica dipende dalla correzione cache della PR #27. Applicare prima
quella correzione, poi questa; costruire/deployare l'immagine MCP secondo il
runbook. Nessun deploy è stato eseguito durante la preparazione del codice.

Seguire [la guida Keycloak/ChatGPT](INSTALLAZIONE_KEYCLOAK_CHATGPT.md): client
`ouf-chatgpt`, utente corretto, tenant e scope `mcp.connect` più
`operations.status.read`, token nuovo. L'ultimo checkpoint live noto resta il
bundle v5 ripristinato dalla v3. Non riutilizzare il grant temporaneo scaduto.

Dopo il deploy preparare tramite il workflow governato un nuovo grant valido
per `ouf.system.status`, soggetto e tenant previsti, con constraints ALLOW,
resourceType `capability` e allowedDetailLevels `[PUBLIC_OPERATIONAL]`.
Conservare tutti i grant amministrativi esistenti, pubblicare e verificare che
la cache acquisisca la nuova versione senza errori oltre la finestra di staleness.

Eseguire poi il tool senza argomenti. Accettare soltanto una risposta reale con
i sei campi sopra, senza contatori o incidenti. Verificare anche le negazioni
per tenant/soggetto diversi, scope mancante e dettaglio non consentito.
Registrare versione bundle, commit/image, correlation ID e audit redatto.
Finché questi passaggi live non riescono, la connessione non è collaudata.
