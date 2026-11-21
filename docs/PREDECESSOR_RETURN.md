# D-080 · Ritorno al predecessore MCP

Il check GitHub Actions `predecessor-return` prova il ritorno su un solo PostgreSQL 17 effimero. Per qualificare una coppia di immagini serve una run verde riferita al commit candidato e al digest effettivamente in esercizio. Il riepilogo della run registra questi riferimenti, gli ID Docker e le versioni dello schema.

## Identità delle immagini

Il predecessore predefinito è `843d22059e03bcd58569fff9960bb46ae552b2b2`, schema 15. La variabile di repository **`MCP_PREDECESSOR_IMAGES`** contiene un oggetto JSON che associa ogni SHA Git completo al riferimento OCI del suo artefatto in esercizio. La forma di una voce è la seguente, con valori segnaposto.

```json
{
  "843d22059e03bcd58569fff9960bb46ae552b2b2": "<registro>/<immagine>@sha256:<64 cifre esadecimali>"
}
```

Usare il digest registrato per il rilascio. Il commit da solo non identifica i byte distribuiti. La preparazione sul runner richiede un riferimento leggibile senza credenziali di progetto. Una voce assente, un tag o un digest irrecuperabile fa fallire il job. Il digest della distribuzione di `843d2205` non è attestato in questa worktree.

Il workflow parte su push e pull request. Per un altro predecessore, selezionare `MCP predecessor return` in Actions e passare la chiave della mappa come input `predecessor` a `workflow_dispatch`. Il candidato è costruito con il Dockerfile del commit selezionato, cioè la testa della PR nel caso di `pull_request`. L'immagine precedente viene risolta una sola volta e le due istanze del suo server devono avere lo stesso ID Docker.

## Sequenza e condizioni di successo

| Fase | Operazioni e verifiche |
|---|---|
| Predecessore | Esegue `ouf-mcp migrate` sul database vuoto. Il server deve dare 204 a `/health/ready`. Tramite MCP crea un'analisi con una task e avvia una `semantic.discovery.request`. Una query collega il tentativo riuscito al claim, all'evento di audit, alla prenotazione riconciliata e alla finestra con consumo registrato. |
| Candidato | Dopo lo stop del predecessore, esegue il proprio `ouf-mcp migrate` sullo stesso database. Il migratore verifica i checksum esistenti e applica eventuali nuove migrazioni. Il candidato crea una seconda analisi e un comando con chiave distinta. |
| Ritorno | Dopo lo stop del candidato, riparte l'immagine iniziale direttamente in modalità `server`. Superata la readiness, `tools/list` deve esporre gli strumenti attesi e il POST operativo a `/api/internal/v1/mcp/operations/status` deve restituire lo stato MCP atteso. Il predecessore rilegge entrambe le analisi e ripete entrambi i comandi con le chiavi originali. |
| Nuova scrittura | Il predecessore esegue un terzo comando e lo ripete. Ogni retry deve riferirsi al risultato originale e a un tentativo riuscito. Il contatore indipendente dell'owner deve riportare una sola esecuzione per chiave. Le righe di audit, budget, idempotenza e tentativo delle prime due fasi devono conservare i riferimenti osservati prima dello stop. |

L'owner di prova serve soltanto bundle di autorizzazione, anteprime sintetiche e ricevute di discovery. Le scritture in PostgreSQL passano dai binari MCP. Dopo la preparazione delle immagini, i container condividono il loopback di un namespace Docker con `--network none`, senza porte pubblicate. I valori di identità sono fixture pubbliche del test. Il database usa ruoli distinti per migrazione e servizio e viene rimosso a fine job.

## Domande aperte

Per la prima run occorre associare al commit `843d2205` il digest della sua immagine in esercizio. Nel sorgente di quel commit `Store.Ready` legge `max(version)` e non impone un massimo supportato. Il candidato del task-25 ha le stesse 15 migrazioni. Se una coppia futura fallisce la readiness al ritorno, il job si ferma prima delle chiamate MCP, registra entrambe le versioni dello schema e mostra il log di avvio. Un rifiuto dello schema lascia aperta D-080 per quella coppia.
