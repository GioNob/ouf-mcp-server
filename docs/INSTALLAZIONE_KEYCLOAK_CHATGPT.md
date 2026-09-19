# Installazione OUF: Keycloak, ChatGPT e autorizzazione MCP

Aggiornamento: 19 settembre 2026. Runbook dell'ambiente `ouf-lab`, basato
sulle evidenze della sessione del 18 settembre e sul codice dei repository.
I valori del laboratorio non sono default universali di prodotto.

## 1. Stato reale e fonti

| Passaggio | Evidenza acquisita | Limite |
|---|---|---|
| Metadata OAuth pubblici | HTTP 200 | Non prova l'esecuzione dei tool |
| `/mcp` senza bearer | HTTP 401 e challenge `resource_metadata` | Comportamento atteso |
| Discovery autenticata | `server/discover`, HTTP 200 | Non prova un grant applicativo |
| Plugin ChatGPT | Account connesso e strumenti elencati | L'esecuzione è rimasta negata |
| Amministrazione HUMAN | `ADMIN_READ_OK`, registrazione capability 201 | Richiede il soggetto amministratore corretto |
| Policy v4 | Pubblicata; respinta dalla cache MCP | Incidente, non configurazione da ripetere |
| Ripristino | v5 PUBLISHED con contenuto derivato da v3 | Esito riportato dall'operatore; nessuna nuova verifica live in questa modifica |
| Correzione cache | [PR MCP #27](https://github.com/GioNob/ouf-mcp-server/pull/27), commit codice `423f279` | Al checkpoint non mergiata né distribuita |
| `PUBLIC_OPERATIONAL` | Correzione preparata, non deployata | Profilo fisso pubblico, autorizzazione e proiezione owner/MCP; collaudo live ancora necessario |

Questa guida non dichiara concluso il collaudo MCP. Un token valido e un elenco
di strumenti non equivalgono a una chiamata autorizzata e completata.

Riferimenti normativi consultati: Reality Baseline Package v1.7; Authorization
PET v1.5 (identità HUMAN/SERVICE, amministrazione delle policy, §34.2);
MCP PET v1.4 (mediazione Gateway e fail-closed); Gateway PET v1.5.
Queste istruzioni descrivono i binding del laboratorio, senza modificare i PET.

Riferimenti implementativi:

- [Bootstrap IAM precedente](https://github.com/GioNob/ouf-source-onboarding/blob/72be8f0472cc8b95188dd5c82c921dfb5d723a66/docs/history/IAM_BOOTSTRAP_PRODUCTION_ACCEPTANCE_2026-09-18.md).
- [Discovery e deploy APISIX](https://github.com/GioNob/ouf-api-gateway/blob/71586ab29823709fb2328313ed74b3c1f666f032/docs/R3A1J_CHATGPT_OAUTH_DISCOVERY.md).
- [Contratto amministrazione policy](https://github.com/GioNob/ouf-source-onboarding/blob/72be8f0472cc8b95188dd5c82c921dfb5d723a66/openapi/authorization-admin-v1.yaml).
- [Incidente cache e vincoli residui](CONSTRAINED_POLICY_CACHE_INCIDENT.md).
- [Guida ufficiale Keycloak](https://www.keycloak.org/docs/latest/server_admin/index.html) e [autenticazione OpenAI](https://developers.openai.com/plugins/build/auth).

## 2. Dove lavorare e quali identità usare

**Browser del PC:** console Keycloak, configurazione ChatGPT e login umano.
**Terminale SSH del server, utente `oufadmin`:** tutti i comandi Bash/Python
di questa guida. Non incollarli nella PowerShell 5.1 del PC.
Non serve un ricevitore OAuth locale sul PC o una nuova porta pubblica.

| Elemento | Valore del laboratorio | Funzione |
|---|---|---|
| Realm applicativo | `ouf` | Utenti e client OUF |
| Realm amministrativo Keycloak | `master` | Console IAM; non creare qui il client ChatGPT |
| Amministratore console osservato | `oufadmin` | Distinto dall'amministratore applicativo |
| Utente OUF amministrativo | `ouf-admin` | Grant `authorization.policy.admin` |
| Utente dedicato ChatGPT | `giovanni-chatgpt` | Solo capability esplicitamente assegnate |
| Client ChatGPT | `ouf-chatgpt` | Login umano Authorization Code + PKCE |
| Client amministrativo | `ouf-human-admin` | Device Flow umano nel laboratorio |
| Client workload | `ouf-mcp-server` | Client Credentials, lettura bundle e chiamate tecniche |
| Client audience Gateway | `ouf-api-gateway` | Destinatario del token; non usarne il secret in ChatGPT |

Il `sub` è l'ID dell'utente, non il client ID né il nome visualizzato.
Nel collaudo i soggetti erano:

- `ouf-admin`: `b93d8cf6-cd14-4ee6-91d7-84cd76c4f500`;
- `giovanni-chatgpt`: `177fd705-b57f-4f9e-a23c-af9d1f3f1f75`.

Su una nuova installazione copiare gli ID effettivi da **Users → utente → Details**;
non riutilizzare questi UUID. L'ID è necessario per il grant applicativo.
La console mostrava un amministratore temporaneo: il passaggio a un admin
permanente è un'attività da verificare, non risulta completato in questa sessione.

## 3. URL e componenti

| Campo | Valore |
|---|---|
| Console Keycloak | `https://auth.ouf-lab.it/admin/` |
| Issuer / base authorization server | `https://auth.ouf-lab.it/realms/ouf` |
| Configurazione OIDC | `https://auth.ouf-lab.it/realms/ouf/.well-known/openid-configuration` |
| Authorization endpoint | `https://auth.ouf-lab.it/realms/ouf/protocol/openid-connect/auth` |
| Token endpoint | `https://auth.ouf-lab.it/realms/ouf/protocol/openid-connect/token` |
| Userinfo | `https://auth.ouf-lab.it/realms/ouf/protocol/openid-connect/userinfo` |
| Device endpoint | `https://auth.ouf-lab.it/realms/ouf/protocol/openid-connect/auth/device` |
| Registration endpoint rilevato | `https://auth.ouf-lab.it/realms/ouf/clients-registrations/openid-connect` |
| URL server del plugin / resource | `https://api.ouf-lab.it/mcp` |
| Protected-resource metadata | `https://api.ouf-lab.it/.well-known/oauth-protected-resource` |
| Callback mostrata da ChatGPT | `https://chatgpt.com/connector_platform_oauth_redirect` |

Copiare sempre la callback visualizzata dalla propria configurazione ChatGPT:
deve coincidere esattamente con la redirect URI registrata, senza wildcard.
Gli endpoint OAuth vanno confrontati con la discovery dell'issuer.

Versioni osservate, non prescrizioni di aggiornamento: Keycloak `26.7.4`,
APISIX `3.18.0-debian`, Caddy `2.11.4`, MCP `33eb219`, Onboarding `72be8f0`,
Gateway `71586ab`. Container: `ouf-keycloak`, `ouf-apisix`, `ouf-caddy`,
`ouf-mcp`, `ouf-onboarding`; rete interna `ouf-backend`.
Non reinstallare servizi e non rigenerare password per ripetere il collaudo.

## 4. Keycloak: creare o verificare `ouf-chatgpt`

Nel browser aprire la console, poi **Manage realms → ouf**. Verificare che
**Current realm** sia `ouf`. Aprire **Clients** e cercare `ouf-chatgpt`.
Se manca: **Create client**, protocollo **OpenID Connect**, client ID
`ouf-chatgpt`, quindi **Next**.

Configurazione da verificare e salvare:

| Campo | Valore |
|---|---|
| Client authentication | On |
| Authorization | Off |
| Standard flow | On |
| Direct access grants | Off |
| Implicit flow | Off |
| Service accounts roles | Off |
| Device Authorization Grant per questo client | Off |
| Require PKCE | On, metodo S256 |
| Require DPoP bound tokens | Off nel binding descritto |
| Valid redirect URIs | Callback esatta della sezione 3 |
| Root URL, Home URL, Admin URL | Vuoti se non richiesti da un altro binding |
| Valid post logout redirect URIs, Web origins | Vuoti per questo flusso server-side |

Premere **Save**. **Credentials → Client secret** contiene il secret da
copiare direttamente nel campo secret di ChatGPT. Non usare la password
dell'utente o il secret del client workload. Non incollare secret nella chat,
nel repository o negli screenshot. Conservare il valore nel gestore di segreti.

L'evidenza della sessione conferma token emessi per `ouf-chatgpt`; non contiene
un export completo finale del client. La tabella è la configurazione da
verificare per riprodurre il binding, non una dichiarazione che ogni switch
sia stato rilevato dopo il salvataggio.

### Scope: creazione e assegnazione sono due passaggi

Nel menu sinistro **Client scopes**, cercare `mcp.connect` e
`operations.status.read`. Per ciascuno, se assente: **Create client scope**:

| Campo | Valore |
|---|---|
| Name | Nome esatto dello scope |
| Type | None, per evitare assegnazioni globali automatiche |
| Protocol | OpenID Connect |
| Include in token scope | On |
| Include in OpenID Provider Metadata | On nel laboratorio |
| Display on consent screen | On; testo descrittivo facoltativo |

Salvare. Poi **Clients → ouf-chatgpt → Client scopes → Add client scope**:
selezionare lo scope e assegnarlo come **Default** al solo client.
Se è già assegnato non compare nel popup di aggiunta: cercarlo nella lista
principale e controllare anche la seconda pagina. Scope OIDC `profile` e
`email` possono restare secondo il binding esistente; `openid` si richiede
nel flusso OIDC, non va inventato come nuovo scope applicativo.

`mcp.connect` ammette al trasporto. `operations.status.read` è richiesto
dalla capability di stato. Nessuno dei due sostituisce un grant nella policy OUF.
Non aggiungere `authorization.policy.admin` al client ChatGPT.

### Audience mapper

**Clients → ouf-chatgpt → Client scopes → ouf-chatgpt-dedicated → Mappers →
Configure a new mapper → Audience**:

| Campo | Valore |
|---|---|
| Name | `ouf-api-gateway-audience` |
| Included Client Audience | `ouf-api-gateway` |
| Included Custom Audience | Vuoto |
| Add to ID token | Off |
| Add to access token | On |
| Add to lightweight access token | Off nel binding descritto |
| Add to token introspection | On |

Premere **Save**. La presenza di `account` accanto a `ouf-api-gateway` in
`aud` è stata osservata; non è necessario eliminarla per questa prova.

## 5. Utente ChatGPT e claim trusted

**Users → Add user** nel realm `ouf`: creare un utente dedicato, ad esempio
`giovanni-chatgpt`, con nome/cognome e un indirizzo email controllato se usato
da OIDC. Non marcare l'email verificata senza averne verificato il possesso.
**Credentials → Set password**: impostare una password privata. Se Temporary
è On, completare il cambio al primo login; per la prova era stata impostata
una password utilizzabile e non risultavano required actions pendenti.

Per rendere gestibili gli attributi custom: **Realm settings → User profile →
Create attribute**, creare separatamente `tenant_id` e `ouf_actor_type`:

| Campo | Valore |
|---|---|
| Attribute [Name] | Nome esatto, case-sensitive |
| Multivalued | Off |
| Default value | Vuoto |
| Attribute group | None |
| Enabled when | Always |
| Required field | Off: non cambiare globalmente tutti gli utenti |
| Who can edit? | Solo Admin; User disabilitato |
| Who can view? | Admin; User non necessario per il binding |
| Validations / Annotations / SCIM | Nessuna aggiunta richiesta per questa procedura |

Questi attributi incidono sull'autorizzazione: non devono essere
autoassegnabili dall'utente. Salvare ogni definizione, tornare a **Users →
giovanni-chatgpt → Details**, valorizzare `tenant_id=ouf-lab` e
`ouf_actor_type=HUMAN`, quindi **Save**. Verificare i valori dopo la riapertura.

Nel dedicated scope di `ouf-chatgpt`, creare due mapper **User Attribute**:

| Campo | Mapper tenant | Mapper actor |
|---|---|---|
| Name | `ouf-tenant-id` | `ouf-actor-type` |
| User Attribute | `tenant_id` | `ouf_actor_type` |
| Token Claim Name | `tenant_id` | `ouf_actor_type` |
| Claim JSON Type | String | String |
| Add to access token | On | On |
| Add to token introspection | On | On |
| Add to ID token / userinfo | Off se non necessario al client | Off se non necessario al client |
| Multivalued / Aggregate attribute values | Off | Off |

Non creare un mapper `Audience` per questi due claim. Non copiare sul client
umano i mapper workload `ouf_subject=workload:...` o `ouf_actor_type=SERVICE`.
Per HUMAN usare il normale `sub`; il runtime supporta il fallback da
`ouf_subject` assente a `sub`. `HUMAN_USER` nelle policy di routing Gateway
è una rappresentazione del confine: non sostituire arbitrariamente il claim
canonico `HUMAN` verificato nel JWT.

Verificare con **Client scopes → Evaluate** o un token fresco, localmente,
senza pubblicare il JWT. I claim attesi sono `iss` corretto, audience Gateway,
`azp=ouf-chatgpt`, `sub` dell'utente dedicato, `tenant_id=ouf-lab`,
`ouf_actor_type=HUMAN`, `acr` presente e scope necessari. `acr=1` è stato
osservato: non costituisce di per sé prova di MFA/step-up.

## 6. Configurare il plugin in ChatGPT

Nel browser ChatGPT aprire la configurazione del plugin **OUF - MCP Server**.

1. **URL del server:** `https://api.ouf-lab.it/mcp`.
2. **Autenticazione:** OAuth.
3. **Impostazioni OAuth avanzate → Metodo di registrazione:**
   **Client OAuth definito dall'utente**.
4. **ID client OAuth:** `ouf-chatgpt`; **Client secret:** quello di questo client.
   Metodo token endpoint: `client_secret_post` nel binding descritto.
5. Confrontare endpoint, issuer e resource con la sezione 3.
6. Mantenere `mcp.connect` selezionato. Per la prova di stato richiedere anche
   `operations.status.read`, ad esempio negli ambiti di base se non appare
   tra quelli predefiniti. Deve essere assegnato anche in Keycloak.
7. Se OIDC è abilitato, usare la discovery del realm e gli scope OIDC
   supportati, normalmente `openid profile email`.
8. Creare/riconnettere il plugin e autenticarsi come `giovanni-chatgpt`.
   Se il browser propone automaticamente `ouf-admin`, cambiare account o
   usare una finestra privata. Il nome email mostrato non basta a provare il `sub`.
9. Aggiornare le azioni e usare **Prova in chat**. Gli strumenti devono essere
   effettivamente disponibili nella conversazione che esegue il collaudo.

La DCR aveva restituito `403 insufficient_scope`, policy `Trusted Hosts`,
`Host not trusted`. Si è scelto il client preregistrato: non è stata risolta
la DCR e non occorre disabilitare Trusted Hosts per questa configurazione.
CIMD risultava non disponibile: non è stato configurato.
Dopo modifiche a scope/mapper ottenere un token nuovo tramite riconnessione.

## 7. Gateway: materializzazione e verifiche pubbliche

**Server SSH, directory `/opt/ouf/gateway`.** Usare il runtime derivato dalla
InstallationProjection attiva. Il file usato nella sessione era
`/tmp/gateway-runtime-8ba82db.json`; è un artefatto storico, non un percorso
da presumere ancora presente o attuale. Verificare prima provenienza e versione.

Esempio storico della materializzazione riuscita dopo correzione permessi:

```bash
cd /opt/ouf/gateway
sudo python3 tools/materialize_apisix_runtime.py \
  --runtime /tmp/gateway-runtime-8ba82db.json \
  --oidc-client-secret-ref '$ENV://OUF_GATEWAY_OIDC_CLIENT_SECRET' \
  --output /tmp/apisix-mcp-71586ab.json
```

Il primo tentativo senza `sudo` diede PermissionError. Usare l'utente
autorizzato a leggere il runtime; non renderlo leggibile a tutti.
Il riferimento `$ENV://...` deve restare letterale. Il secret effettivo resta
nel binding APISIX. Verificare le due route prima del deploy:

```bash
sudo python3 -c 'import json; d=json.load(open("/tmp/apisix-mcp-71586ab.json")); print([(r["id"],r["methods"],r["uri"],sorted(r.get("plugins",{}))) for r in d["routes"]])'
```

Per una ripetizione approvata del deploy, dopo aver verificato il file runtime:

```bash
sudo env OUF_APISIX_MATERIALIZATION=/tmp/apisix-mcp-71586ab.json \
  OUF_APISIX_ADMIN_KEY_FILE=/opt/ouf/secrets/apisix-admin-key \
  OUF_APISIX_CONTAINER=ouf-apisix \
  sh ops/apisix/deploy_mcp_route.sh
```

Esito osservato: `APISIX_MCP_ROUTES_ACTIVE mcp_id=public-mcp-endpoint
metadata_id=public-mcp-oauth-protected-resource negative_http=401`.
Lo script effettua snapshot, read-back e rollback delle route in caso di gate
fallito. Non modificarle manualmente per aggirare gli errori.

Verifiche di sola lettura, eseguibili sul server:

```bash
curl --max-time 20 -sS -D - https://api.ouf-lab.it/.well-known/oauth-protected-resource
curl --max-time 20 -sS -D - -o /dev/null \
  -H 'Content-Type: application/json' --data-binary '{}' https://api.ouf-lab.it/mcp
```

Attesi: metadata 200 con issuer/resource corretti e `mcp.connect`; seconda
richiesta 401 con `WWW-Authenticate: Bearer resource_metadata="https://api.ouf-lab.it/.well-known/oauth-protected-resource", scope="mcp.connect"`.
Il metadata pubblicizza lo scope di trasporto, non tutti i grant applicativi.

La discovery autenticata è stata verificata con protocollo `2026-07-28`,
header `Mcp-Method: server/discover` e bearer da file privato. Ha restituito
`ouf-mcp-server`, capability tools e HTTP 200. I tre file temporanei di quella
prova sono stati rimossi dall'operatore: non presumerli ancora disponibili.

## 8. Accesso amministrativo umano sul server: Device Flow

Il client **ouf-human-admin** è separato dal client ChatGPT:
Client authentication Off, Standard flow Off, Direct access grants Off,
OAuth 2.0 Device Authorization Grant On. Scope amministrativo
`authorization.policy.admin`, audience `ouf-api-gateway`, tenant `ouf-lab`,
actor `HUMAN`. Conservare gli altri scope di installazione già previsti.
`authorization.bootstrap` era stato rimosso dopo il bootstrap: non riaggiungerlo.
Non occorrono redirect URI, callback locali o modifiche allo Standard flow.

Il flusso sotto è una procedura riproducibile equivalente a quella eseguita,
non il recupero del vecchio script temporaneo. Avvia la richiesta dal server;
il login avviene nel browser come **ouf-admin**. Salva solo in una directory
root privata, gestisce polling e scadenza e non stampa token. Il controllo del
payload serve a evitare l'account sbagliato; la validazione crittografica e
l'autorizzazione restano responsabilità del server destinatario.

**Server SSH:**

```bash
sudo python3 - <<'PY'
import base64, json, os, pathlib, tempfile, time
import urllib.error, urllib.parse, urllib.request
os.umask(0o077)
root = pathlib.Path(tempfile.mkdtemp(prefix="ouf-admin-device-", dir="/run"))
base = "https://auth.ouf-lab.it/realms/ouf/protocol/openid-connect"
def post(path, data):
    req = urllib.request.Request(base + path, data=urllib.parse.urlencode(data).encode())
    try:
        with urllib.request.urlopen(req, timeout=20) as res:
            return json.load(res)
    except urllib.error.HTTPError as err:
        return json.load(err)
def save(name, value):
    tmp = root / (name + ".new")
    tmp.write_text(json.dumps(value))
    tmp.replace(root / name)
state = post("/auth/device", {"client_id":"ouf-human-admin", "scope":"openid authorization.policy.admin"})
if "device_code" not in state:
    raise SystemExit("Device request failed: " + str(state.get("error", "invalid response")))
save("state.json", state)
print("STATE_FILE=" + str(root / "state.json"), flush=True)
print("Aprire nel browser: " + state["verification_uri"], flush=True)
print("Codice: " + state["user_code"] + " — autenticarsi come ouf-admin", flush=True)
deadline = time.monotonic() + int(state["expires_in"])
interval = max(1, int(state.get("interval", 5)))
while time.monotonic() < deadline:
    time.sleep(interval)
    token = post("/token", {"client_id":"ouf-human-admin", "grant_type":"urn:ietf:params:oauth:grant-type:device_code", "device_code":state["device_code"]})
    if "access_token" in token:
        part = token["access_token"].split(".")[1]
        claims = json.loads(base64.urlsafe_b64decode(part + "=" * (-len(part) % 4)))
        save("token.json", token)
        if claims.get("sub") != "b93d8cf6-cd14-4ee6-91d7-84cd76c4f500" or claims.get("ouf_actor_type") != "HUMAN" or claims.get("tenant_id") != "ouf-lab":
            raise SystemExit("Account inatteso: nessuna chiamata admin effettuata. Chiudere la sessione IAM e rimuovere questa directory temporanea.")
        (root / "admin.header").write_text("Authorization: Bearer " + token["access_token"] + "\n")
        print("ADMIN_TOKEN_OK; EXPIRES_IN=" + str(token["expires_in"]), flush=True)
        break
    error = token.get("error")
    if error == "slow_down":
        interval += 5
    elif error != "authorization_pending":
        raise SystemExit("Device polling stopped: " + str(error))
else:
    raise SystemExit("Device code expired; avviare una nuova richiesta")
PY
```

Per una nuova installazione sostituire nello script l'UUID amministratore
con quello del grant approvato. Non inserire l'UUID dell'utente ChatGPT.
Nel collaudo il primo login aveva emesso un token per `giovanni-chatgpt`,
producendo 403; un nuovo Device Flow con `ouf-admin` ha risolto quel 403.

Impostare `OUF_ADMIN_DIR` al percorso appena stampato, senza `/state.json`.
Il comando seguente è una lettura autenticata nella rete Docker; non pubblica
porte, non rende trusted la richiesta solo perché interna:

```bash
OUF_ADMIN_DIR=/run/ouf-admin-device-SOSTITUIRE
sudo docker run --rm --network ouf-backend \
  --user 0:0 -v "$OUF_ADMIN_DIR:/auth:ro" curlimages/curl:8.16.0 \
  --max-time 20 -sS -D - -H @/auth/admin.header \
  http://ouf-onboarding:8080/api/trusted-human/v1/authorization/capabilities
```

Atteso HTTP 200; `[]` è un risultato valido della lista registrazioni e non
prova che il bundle ACTIVE sia vuoto. Non confondere i due endpoint.
Il bearer dura pochi minuti (300 secondi osservati). Se scaduto, ripetere
il Device Flow o usare il refresh previsto dal client salvando il nuovo
token atomicamente; non sovrascrivere il file valido con una risposta di errore.

## 9. Policy: registrazione, draft, pubblicazione e rollback

Le mutazioni sono amministrazione umana, escluse dagli strumenti MCP.
Usare il canale trusted-human autenticato; non fare INSERT/UPDATE SQL, non
aggiungere header che dichiarino arbitrariamente identità o CSRF validati.
L'adapter IAM stabilisce il contesto trusted e la prova di scrittura.

### Prima di cambiare una policy

1. Leggere e salvare l'intero envelope ACTIVE attraverso la route workload
   `GET /internal/capabilities/v1/authorization/policy-bundle/active` del Gateway,
   con token fresco di `ouf-mcp-server` e scope `authorization.bundle.read`.
   Conservare `contentHash`, `bundleId`, `bundleVersion`, `activatedAt`, `bundle`.
   La lettura non usa il token umano dell'admin.
2. Conservare il backup nella directory privata dell'operazione. Verificare
   che sia JSON valido e riferito alla versione attiva corrente.
3. Identificare soggetto, tenant, scope, capability, validità e dettaglio del
   nuovo grant. Conservare integralmente tutte le altre capability e grant,
   compreso il grant amministrativo che evita il lockout.
4. Verificare prima che il consumer accetti quei vincoli. Per lo stato
   `PUBLIC_OPERATIONAL` richiede sia la correzione cache sia la proiezione descritta
   in [PUBLIC_OPERATIONAL_STATUS.md](PUBLIC_OPERATIONAL_STATUS.md). Sul server
   non risultano ancora deployate: non ripubblicare il grant di prova seguendo
   soltanto l'esempio storico sotto.

### Backup ACTIVE tramite identità workload

Per acquisire il backup ACTIVE del punto 1 senza stampare credenziali,
**server SSH**, usare il percorso privato scelto per questa operazione.
Il secret workload viene letto dal file esistente; non viene rigenerato.

```bash
sudo python3 - "$OUF_ADMIN_DIR" <<'PY'
import json, os, pathlib, sys, urllib.parse, urllib.request
os.umask(0o077)
root = pathlib.Path(sys.argv[1]).resolve()
if root.parent != pathlib.Path('/run') or not root.name.startswith('ouf-admin-device-') or not root.is_dir():
    raise SystemExit('Directory privata operazione non valida')
dest = root / 'active-before.json'
if dest.exists():
    raise SystemExit('Backup gia presente: conservarlo; scegliere un nuovo percorso per una nuova operazione')
secret = pathlib.Path('/opt/ouf/secrets/mcp-client-secret').read_text().strip()
form = urllib.parse.urlencode({'grant_type':'client_credentials', 'client_id':'ouf-mcp-server', 'client_secret':secret}).encode()
request = urllib.request.Request('https://auth.ouf-lab.it/realms/ouf/protocol/openid-connect/token', data=form)
with urllib.request.urlopen(request, timeout=20) as res:
    token = json.load(res)['access_token']
request = urllib.request.Request('https://api.ouf-lab.it/internal/capabilities/v1/authorization/policy-bundle/active', headers={'Authorization':'Bearer ' + token})
with urllib.request.urlopen(request, timeout=20) as res:
    active = json.load(res)
if not all(k in active for k in ('contentHash','bundleId','bundleVersion','activatedAt','bundle')):
    raise SystemExit('Envelope ACTIVE incompleto')
with dest.open('x') as out:
    json.dump(active, out, indent=2)
print('ACTIVE_BACKUP_OK', active['bundleId'], active['bundleVersion'])
PY
```

Se l'endpoint risponde con errore, correggere il percorso autorizzato di
distribuzione: non usare una copia parziale o un bundle inventato come backup.
Il controllo di hash e schema del consumer resta obbligatorio; questo comando
acquisisce la risposta TLS e controlla solo la presenza dei campi dell'envelope.

### Registrazione osservata (HTTP 201)

`POST /api/trusted-human/v1/authorization/capabilities`, body:

```json
{
  "ownerRef": "mcp",
  "descriptor": {
    "capabilityId": "ouf.system.status",
    "operation": "READ",
    "requiredScope": "operations.status.read",
    "allowedActors": ["HUMAN"]
  }
}
```

La registrazione è immutabile: se esiste, confrontarla con il descrittore
desiderato; non trattare ogni 409 come successo. Il rollback del bundle non
cancella automaticamente questa registrazione.

### Forma delle richieste e comandi

Creazione draft: `POST /api/trusted-human/v1/authorization/policies` con il
**PolicyBundle completo**, non con l'envelope di distribuzione. Campi:
`bundleId`, nuova `version`, `publishedAt`, array completi `capabilities` e
`grants`. `baseActiveRef` è catturato dal server nella risposta Draft, non va
aggiunto al body PolicyBundle. La risposta contiene `id`, `revision`, `state`,
`baseActiveRef`, `policy`; conservare anche ETag.

Preparare nella directory privata `registration.json` oppure `policy.json`.
Questi comandi modificano lo stato: eseguirli solo per il contenuto già
revisionato, dopo i gate sopra. Sostituire la directory con quella corrente.

```bash
# Registrazione: usare solo se non è già presente e coerente.
sudo docker run --rm --network ouf-backend --user 0:0 \
  -v "$OUF_ADMIN_DIR:/auth:ro" curlimages/curl:8.16.0 \
  --max-time 20 -sS -D - -H @/auth/admin.header \
  -H 'Content-Type: application/json' --data-binary @/auth/registration.json \
  http://ouf-onboarding:8080/api/trusted-human/v1/authorization/capabilities

# Crea il draft completo; non pubblica ancora.
sudo docker run --rm --network ouf-backend --user 0:0 \
  -v "$OUF_ADMIN_DIR:/auth:ro" curlimages/curl:8.16.0 \
  --max-time 20 -sS -D - -H @/auth/admin.header \
  -H 'Content-Type: application/json' --data-binary @/auth/policy.json \
  http://ouf-onboarding:8080/api/trusted-human/v1/authorization/policies
```

Leggere il draft restituito: `GET .../policies/{id}`. Prima della pubblicazione
verificare diff, validità temporale e `baseActiveRef`. Pubblicare con
`POST .../policies/{id}:publish` e `If-Match` uguale alla revisione **quotata**
effettiva. Esempio per un draft ancora a revisione 0:

```bash
OUF_DRAFT_ID=UUID_DEL_DRAFT_VERIFICATO
sudo docker run --rm --network ouf-backend --user 0:0 \
  -v "$OUF_ADMIN_DIR:/auth:ro" curlimages/curl:8.16.0 \
  --max-time 20 -sS -D - -X POST -H @/auth/admin.header \
  -H 'If-Match: "0"' \
  "http://ouf-onboarding:8080/api/trusted-human/v1/authorization/policies/$OUF_DRAFT_ID:publish"
```

Se la revisione non è 0, usare quella corrente. Su 409 per ACTIVE cambiato,
ricostruire il draft dalla nuova ACTIVE; su 412 rileggere ETag/revisione.
Non ritentare ciecamente e non abbassare la versione. Il publish deve restituire
`PUBLISHED`; poi rileggere ACTIVE e verificare il refresh nei consumer.

### Storia dell'incidente, non template di produzione

- v3: checkpoint precedente; il grant amministrativo era presente. Il primo
  estratto con array vuoti non descriveva adeguatamente il bundle completo.
- v4: draft `4e62ed1c-2e99-4bc3-ab10-756c9f093887`, revisione 0 → 1 PUBLISHED;
  aggiunto `grant-system-status-giovanni-chatgpt`, capability `ouf.system.status`,
  tenant `ouf-lab`, soggetto dell'utente dedicato, vincoli
  `effect=ALLOW`, `resourceType=capability`,
  `allowedDetailLevels=[PUBLIC_OPERATIONAL]`.
- Finestra storica del grant: `2026-09-18T21:53:59.102323Z` →
  `2026-09-19T21:53:59.102323Z`. Non riutilizzarla per una nuova prova.
- MCP ha rifiutato la copia JSON del bundle con
  `blank or invalid grant constraint`; dopo la scadenza della cache i tool
  hanno restituito `authorization policy bundle unavailable`.
- v5: nuova pubblicazione monotona che ripristina i contenuti v3.
  Evidenza operatore: `state=PUBLISHED`, `version=5`, `restoredFromVersion=3`.
  Non equivale a rimettere il puntatore ACTIVE su v3.

Rollback generale: usare il contenuto del backup verificato come nuova policy,
con la **stessa lineage** e una versione maggiore della corrente; far catturare
dal server il riferimento ACTIVE corrente creando un nuovo draft, revisionarlo
e pubblicarlo con ETag. Preservare accesso amministrativo e controlli consumer.
Non riaprire il bootstrap latch. Nessun contenuto ACTIVE va modificato in-place.

## 10. Diagnostica e criteri di accettazione

| Sintomo | Verifica / intervento |
|---|---|
| DCR 403 Trusted Hosts | Usare il client preregistrato descritto; non dichiarare DCR risolta |
| Campo redirect assente | Controllare client/realm; per Device Flow admin non serve; per ChatGPT controllare Standard flow |
| Scope non trovato | Distinguere scope del realm, scope assegnati e popup Add; togliere filtri e controllare paginazione |
| Attributo utente non valorizzato | Definire User profile, impostare il valore utente, salvare, verificare mapper e token fresco |
| Admin 403 | Controllare `sub`, actor, tenant, scope e grant; nel caso osservato era l'utente ChatGPT |
| Plugin connesso, zero azioni | Aggiornare azioni e controllare errore reale nei log; usare Prova in chat |
| `authorization denied` | Scope/token e grant sono controlli distinti; non assegnare privilegi admin per risolvere |
| `blank or invalid grant constraint` | Difetto cache documentato nella PR #27; ripristino monotono e fix revisionato |
| `authorization policy bundle unavailable` | Verificare fetch, hash, validazione e freschezza; riavviare non corregge un bundle invalido |
| `/readyz` restituisce 404 | Endpoint MCP reali: `/health/live`, `/health/ready`; readiness non prova il caricamento policy |
| Comando senza output | Controllare exit code/HTTP: silenzio non è una prova di successo |

**Server SSH**, lettura limitata dei log dopo una prova:

```bash
sudo docker logs --since 5m --tail 80 ouf-mcp
```

Prima di dichiarare concluso: verificare refresh del bundle corretto oltre
la finestra di staleness (default 5 minuti, refresh 30 secondi), token nuovo,
utente giusto, scope, grant valido e una chiamata reale. La prova positiva
deve preservare negazioni per altro soggetto/tenant, scope mancante e dettaglio
non consentito. Non registrare bearer o password nei log di collaudo.

Il tool `ouf.system.status` conserva lo schema senza parametri. La correzione
[PUBLIC_OPERATIONAL_STATUS.md](PUBLIC_OPERATIONAL_STATUS.md) imposta il dettaglio
pubblico nel contesto interno prima dell'autorizzazione e limita la risposta
sia nell'API owner sia dopo il Gateway. Il prompt non seleziona il dettaglio.
Il profilo fisso non restituisce contatori né riferimenti agli incidenti.
Il server resta **BLOCCATO fino a deploy e collaudo live**: non eliminare i
vincoli dal grant per superare una negazione.

## 11. Segreti, chiusura sessione e passaggio di consegne

I segreti rimangono nella gestione esistente (`/opt/ouf/secrets` nel laboratorio).
`keycloak-admin-password` riguarda l'accesso console iniziale;
`mcp-client-secret` riguarda il workload; `apisix-admin-key` il deploy route.
Non scambiare questi valori col secret di `ouf-chatgpt`. Cambiare una password
in Keycloak non prova che il vecchio file bootstrap sia stato aggiornato:
allineare il registro dei segreti secondo la procedura IAM vigente, senza
reinstallare Keycloak o rieseguire il bootstrap.

Le directory `/run/ouf-admin-device-*` contengono device code, access token
e possibilmente refresh token: non sono prove da pubblicare. Al termine,
chiudere/revocare la sessione admin in Keycloak e rimuovere solo la directory
esatta dell'operazione dopo aver verificato il percorso; niente cancellazioni
con wildcard. La rimozione dei file non revoca una sessione IAM.
Conservare separatamente solo evidenze redatte: timestamp UTC, commit/image,
HTTP status, versione/hash policy, revisioni draft, correlation ID e risultato.

Il checkpoint di questa guida lascia il server al ripristino riportato v5.
La PR #27 corregge la cache e contiene regressioni; merge, deploy e successivo
collaudo live sono passaggi separati ancora da registrare. Le nuove istruzioni
sono state confrontate con codice/contratti e controllate sintatticamente;
non sono state rieseguite sul server durante la redazione.

## 13. Ripresa del 19 settembre: stato e delega Gateway

Checkpoint osservato, non una dichiarazione di deploy delle modifiche successive:

- MCP `8c4046a` avviato; container precedente conservato come
  `ouf-mcp-rollback-33eb219`. Inspect originale in
  `/run/ouf-mcp-backup-re8wb1zo.json` (temporaneo, perso al reboot).
- Policy `ouf-lab-authorization:6` pubblicata, draft
  `0fd3b6c7-653e-4091-9653-e1066d12fc79`, revisione 1. Mantiene i grant
  precedenti e aggiunge la lettura pubblica per `giovanni-chatgpt`.
- Grant di prova valido fino al **20 settembre 2026, 05:27:26 UTC**.
  Alla scadenza serve una nuova pubblicazione autorizzata; non disattivare i
  controlli temporali e non trasformare automaticamente il grant in permanente.
- La creazione del draft ha restituito **HTTP 200** con `state: DRAFT`.
  Non ripeterla perché si aspettava 201: conservare ID, revisione ed ETag,
  rileggere il draft e verificare il contenuto prima del publish.
- La chiamata successiva ha restituito `STATUS_UNAVAILABLE`, HTTP 404.
  APISIX esponeva `/mcp`, metadata OAuth, lettura bundle e due rotte legacy
  per related-search, ma mancava **POST `/internal/capabilities/v1/execute`**.

### Correzione coordinata MCP e Gateway

Usare insieme questa versione MCP e la materializzazione descritta in
[`ouf-api-gateway/docs/MCP_STATUS_EXECUTE_DEPLOYMENT.md`](https://github.com/GioNob/ouf-api-gateway/blob/fix/mcp-execute-mediation/docs/MCP_STATUS_EXECUTE_DEPLOYMENT.md).
La nuova rotta è deliberatamente limitata a `ouf.system.status`, READ,
argomenti vuoti e soggetto HUMAN. Non significa che tutto il catalogo MCP sia
eseguibile. La rotta non usa `/mcp` come backend: arriva all'API owner
`/api/internal/v1/mcp/operations/status` senza ricorsione di protocollo.

Dopo la verifica OIDC del token umano, il Gateway emette una prova opaca
`X-OUF-Delegation`, valida al massimo 60 secondi e mai oltre la scadenza del
JWT originale. È vincolata a issuer, audience, workload, tenant, soggetto,
client, scope e contesto di autenticazione. La chiave dedicata rimane solo
nel Gateway; MCP non la riceve e non può emettere prove.

MCP conserva la prova soltanto nel contesto della richiesta, esclusa dalla
serializzazione JSON e dalle registrazioni di audit/ammissione, e la inoltra
come header insieme al proprio token workload. Il Gateway verifica entrambi,
confronta l'identità nell'envelope e ricostruisce gli header owner. Non inoltra
al backend né bearer né prova. L'owner MCP rivaluta il bundle locale prima
di leggere lo stato e richiede la medesima decisione pubblica e lo stesso tenant.

La prova attesta l'identità delegata, **non** sostituisce grant, budget,
ammissione, idempotenza o autorizzazione owner. La traccia di ammissione resta
responsabilità del workload MCP; il Gateway verifica la coerenza dei riferimenti.
Per questo profilo, il client iniziatore è confrontato con il campo storico
`ServicePrincipalID` dell'envelope; verso l'owner quel campo è ricostruito con
il workload autenticato. Il grant di prova è vincolato al soggetto umano.

In produzione `RequireDelegation` è attivo: una chiamata senza prova viene
negata prima dell'ammissione. Deploy parziale o bundle stale falliscono chiusi.
Gli identificativi di correlazione e idempotenza mancanti sono generati per
singola invocazione; quelli forniti dal chiamante sono conservati.

### Verifica dopo il deploy coordinato

1. Verificare versione MCP e presenza della rotta execute APISIX.
2. Verificare refresh del bundle v6 senza errori di vincoli e grant non scaduto.
3. Riconnettere ChatGPT come **giovanni-chatgpt**, non `ouf-admin`.
4. Chiamare `ouf_system_status` senza argomenti: il profilo è già
   `PUBLIC_OPERATIONAL`, non esiste un parametro per aumentare il dettaglio.
5. Registrare il risultato effettivo. Sono ammessi soltanto `module`, `status`,
   `actionRequired`, `partial`, `visibilityClass`, `redacted`.

La presenza degli strumenti o una risposta HTTP di connessione non provano
l'esecuzione della capability. Il gate finale è la chiamata autenticata reale.

### Riferimenti PET e test

PET MCP §37: separazione token umano/workload e assenza di token persistiti;
§83: delega verificabile e rivalutazione fine-grained owner. PET Gateway T29.1:
controlli coarse e mediazione, con proiezione pubblica effettuata dall'owner.
I test coprono prova solo in header, diniego prima dell'ammissione, revoca o
cambio della decisione owner, tenant diverso e dettaglio non pubblico.
Il repository Gateway esegue anche un gate con APISIX 3.18 reale, JWT firmati
con chiavi esclusivamente di test e JWKS locale alla CI.
