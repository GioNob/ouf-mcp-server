# Accesso per ruoli e amministrazione dei permessi

L'accesso ordinario usa i ruoli attestati dall'IAM e le policy OUF che li
associano alle capability. Il login non pubblica grant e non rinnova le policy.
L'admin gestisce i permessi OUF; assegnare un ruolo a una persona resta una
responsabilità dell'IAM. Non viene creato un archivio locale degli utenti.

Autorità: PET Authorization v1.5 §§6.3–7.2, 11–14, 34.2; PET MCP v1.4
§§37, 83–84 e Operational Awareness §33. Roadmap di coordinamento:
[gap register v1.7](https://github.com/GioNob/ouf-semantic-registry/blob/main/evidence/ouf-pet-gap-register-v1.7.json),
AUT-03/04 e MCP-01/02. Questo incremento non chiude da solo questi gruppi.

## Contratto tra IAM Gateway e servizi

1. Il token di accesso firmato dall'IAM contiene `externalRoleRefs`, un array
   di identificatori stabili. Il mapping IAM seleziona i ruoli applicativi
   pertinenti al client, senza attribuire automaticamente privilegi OUF ai
   ruoli predefiniti di Keycloak.
2. Dopo la validazione OIDC, il Gateway valida e ordina i ruoli, li include
   nella delega firmata e ricostruisce `X-OUF-External-Role-Refs` per MCP.
   Header omonimi del client sono eliminati. Il contesto non viene derivato
   da argomenti del tool né da un nome utente.
3. MCP carica i ruoli nel contesto request-only dell'evaluator. Non serializza
   questi claim o la prova di delega nei payload persistiti dell'attempt.
4. Sull'execute, il Gateway verifica la firma e ricostruisce i ruoli per
   l'owner dalla prova. L'owner rivaluta localmente la policy attiva.

Limiti del profilo: massimo 32 ruoli, identificatore ASCII di 1–128 caratteri
nel set `A-Z a-z 0-9 _ : . / -`, nessun duplicato. Il claim assente produce
zero ruoli, non un ruolo predefinito. Claim malformato nega la richiesta.
Il claim `realm_access.roles` o `resource_access` non viene interpretato
implicitamente: il mapper IAM deve emettere il claim canonico esplicito.
Questi header sono utilizzabili soltanto sulle porte private isolate al
Gateway; `X-OUF-Gateway-Verified` da solo non autentica una connessione.

## Configurazione ordinaria nel laboratorio

Ruolo applicativo proposto: `ouf-operations-viewer`. In Keycloak creare o
riusare un ruolo governato con quel riferimento; assegnarlo a Giovanni
nell'IAM. Configurare per `ouf-chatgpt` un role mapper verso l'access token,
claim JSON multivalore `externalRoleRefs`, che riporti gli identificatori
effettivamente assegnati. Non usare un attributo modificabile dall'utente o
un hardcoded claim per dichiarare il ruolo. Il valore emesso deve coincidere
esattamente con `constraints.externalRoleRef` della policy OUF.

Verificare il nuovo access token localmente, senza incollarne il bearer:

```json
{"externalRoleRefs":["ouf-operations-viewer"],"tenant_id":"ouf-lab"}
```

In OUF il grant di ruolo per `ouf.system.status` usa:

| Campo | Valore |
| --- | --- |
| tenantId | `ouf-lab` |
| subjectId | `null` |
| servicePrincipalId | `null` |
| constraints.externalRoleRef | `ouf-operations-viewer` |
| constraints.effect | `ALLOW` |
| constraints.resourceType | `capability` |
| constraints.allowedDetailLevels | `["PUBLIC_OPERATIONAL"]` |

Restano necessari descriptor HUMAN/READ e scope `operations.status.read`.
Il contratto del bundle richiede `validFrom` e `validUntil`: la finestra
amministrativa va scelta esplicitamente e sottoposta a revisione periodica.
Non coincide con la durata del token e non deve essere rinnovata a ogni login.
Non impostare una scadenza automatica di 24 ore per l'abilitazione ordinaria.

La sostituzione del grant personale di prova richiede backup dell'ACTIVE,
nuova versione che conservi tutti gli altri grant e capability, verifica del
diff e pubblicazione umana autenticata. Rimuovere il grant personale di prova
nella stessa versione: lasciarlo attivo renderebbe inefficace il test di revoca
del solo ruolo. Non aggiungere scope o grant amministrativi a Giovanni per
consentirgli una lettura operativa.

## Ordine di rilascio e verifica

1. Rilasciare il consumer MCP con il parser dei ruoli, conservando il rollback.
2. Rigenerare e installare le rotte Gateway con la nuova delega. Riutilizzare
   chiave, configurazione e procedura di backup già collaudate nella
   [guida Gateway](https://github.com/GioNob/ouf-api-gateway/blob/main/docs/MCP_STATUS_EXECUTE_DEPLOYMENT.md).
3. Configurare il mapping IAM e ottenere un nuovo token. Il token e la prova
   emessi prima della modifica non acquisiscono retroattivamente i ruoli.
4. Pubblicare la policy di ruolo attraverso il canale umano amministrativo,
   preservando l'abilitazione dell'amministratore.
5. Provare `ouf_system_status` con il ruolo; poi con un nuovo token privo del
   ruolo. Verificare anche un altro tenant e lo scope mancante.
6. Verificare la revoca del grant con una nuova versione del bundle e il
   refresh dei consumer. Registrare versione, esiti e tempi effettivi.

La rimozione del ruolo IAM è visibile al rinnovo del token; un JWT già emesso
può restare valido fino alla sua scadenza. La delega scade entro 60 secondi e
comunque entro `exp` del token originario. La revoca della policy OUF dipende
dal refresh del bundle (default 30 secondi); se il registry non è raggiungibile,
il last-known-good resta utilizzabile solo entro la max-staleness configurata
(default 300 secondi). Non promettere revoca istantanea. Per emergenze seguire
il runbook IAM e policy, misurando la propagazione.

## Amministrazione conversazionale dei permessi

Esperienza richiesta dall'utente: l'admin chiede quali ruoli autorizzano una
capability, vede scope e vincoli, prepara assegnazioni/revoche ruolo→capability,
simula l'effetto e conferma una modifica esatta nella Trusted Human Surface.
La chat deve poi poter consultare l'esito e la versione pubblicata.

Esistente in Onboarding: registrazione capability, lettura, draft/grant CRUD,
revisioni ETag, pubblicazione e audit tramite
`/api/trusted-human/v1/authorization`. Il boundary corrente richiede HUMAN
e `authorization.policy.admin` anche per creare draft. Non è un'API delegabile
da riutilizzare aggiungendo header che dichiarino un amministratore.

Da implementare come incremento distinto, riusando il dominio esistente:

- Letture delegate bounded del catalogo e delle mappature di ruolo, con
  capability amministrative dedicate e nessuna credenziale esposta.
- Proposta e simulazione autorizzate via MCP, senza effetto sull'ACTIVE.
- Scheda THS con diff, tenant, ruolo, capability, vincoli, durata e impatto;
  conferma legata a hash/revisione/base ACTIVE e controllo dei privilegi
  dell'approvatore. Eventuale separazione proposer/approver è una policy.
- Pubblicazione/revoca umana, audit e stato interrogabile; negare modifiche
  stale, auto-escalation non autorizzata e conferme provenienti dal tool.

`approve/publish/activate` human-only non diventano tool-eligible. Nessun
accesso Keycloak Admin API o gestione account è introdotto da questo requisito.

## Stato verificato e lavoro residuo

Il 19 settembre 2026 `ouf_system_status` ha restituito HEALTHY con la policy
personale di prova. Il test live successivo di `ouf_operations_summary` con
`limit=5` ha restituito `authorization denied` non ritentabile.

Il nuovo codice verifica localmente ruoli, assenza del ruolo, tenant/scope,
revoca della policy e riesame dell'owner. Gateway CI esercita token RS256,
JWKS e delega con APISIX reale. Il rilascio/configurazione IAM e la prova live
con grant di ruolo restano separati dal superamento dei test di codice.

Per `ouf_operations_summary` non basta aggiungere un grant: il profilo execute
deployato accetta soltanto `ouf.system.status`. Servono dispatch bounded del
riepilogo, enforcement owner, contesto delegato nelle chiamate ai producer,
route/grant dei producer e distinzione tra diniego, indisponibilità e assenza
di incidenti, come richiesto da OA-MCP. Non abilitare il riepilogo aggirando
la route chiusa o concedendo privilegi amministrativi al client.
