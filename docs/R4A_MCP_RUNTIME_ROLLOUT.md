# R4a MCP: build, staging e rollback sul laboratorio

Stato al 24 settembre 2026: `urban.object.search` rimane **INACTIVE** nel manifest. Questa procedura prepara il binario con il binding verso la route Gateway già installata. La prova positiva con grant HUMAN, decisione owner e token workload non è ancora stata eseguita. R-INSTALL resta **OPEN**. I PET v1.7 (MCP PET v1.4, Authorization PET v1.5 e Gateway PET v1.5) governano l'ordine dei gate: mediazione Gateway, autorizzazione, fail-closed e nessuna pubblicazione della capability sulla base della sola prova 401.

Tutti i comandi qui sotto sono per **il terminale SSH sul server come `oufadmin`**, non per PowerShell. Docker è invocato tramite `sudo`. Non stampare, copiare in chat o committare snapshot, file `mcp.env`, mount sources o valori dei secret.

## Condizioni iniziali e provenienza

- Repository sul server: `/opt/ouf/mcp`; branch di lavoro `codex/r4a-object-search-mcp`, PR [#42](https://github.com/GioNob/ouf-mcp-server/pull/42).
- `ouf-mcp` attivo come `10005:10005` sulla rete `ouf-backend`, restart `unless-stopped`, due mount in sola lettura ai target `/run/secrets/mcp-client-secret` e `/run/secrets/mcp-fingerprint-key`.
- Il backup root `/opt/ouf/backup` deve essere root:root e 0700. Non creare un altro secret; lo snapshot conserva i binding effettivi.
- Verificare che la CI della PR sia verde per il commit usato. Tutti gli script eseguono un controllo dell'identità del container e si arrestano su impostazioni Docker inattese.

I due percorsi `/opt/ouf/backup/r4a-mcp-runtime-4i2mfgwn/...` qui sotto sono **artefatti del laboratorio**, non valori da copiare su un nuovo host. Su un altro host prendere i percorsi stampati dai primi due script. Anche il digest immagine fissato negli script di staging vale per questa build lab: per un nuovo ambiente servono un artefatto verificato e una parametrizzazione documentata; questo è un residuo di R-INSTALL.

## 1. Build immagine candidata

La ricetta `Dockerfile` è committata. Il commit sorgente verificato è `fbd0e8c0cc22c127ded04ffc658269699fb433ae`; l'immagine risultante del lab è `sha256:ac64e6e7220a9fcc614b6b3dd90a30e01513bab804dbc3f462cfc68f6c6c999a`.

```bash
git -C /opt/ouf/mcp fetch origin codex/r4a-object-search-mcp
test "$(git -C /opt/ouf/mcp rev-parse FETCH_HEAD)" = fbd0e8c0cc22c127ded04ffc658269699fb433ae
set -o pipefail
git -C /opt/ouf/mcp archive fbd0e8c0cc22c127ded04ffc658269699fb433ae | sudo docker build -t ouf-mcp:r4a-fbd0e8c -
sudo docker image inspect --format '{{.Id}} user={{.Config.User}}' ouf-mcp:r4a-fbd0e8c
```

L'ultimo comando deve mostrare il digest sopra e `user=10005:10005`. La build non avvia il container.

## 2. Snapshot privato e preflight

Gli script del branch sono invocati con `git show` su commit immutabili per evitare di modificare il checkout del server. Recuperare il commit documentato, poi usare lo stesso SHA per gli script che contiene:

```bash
git -C /opt/ouf/mcp fetch origin codex/r4a-object-search-mcp
test "$(git -C /opt/ouf/mcp rev-parse FETCH_HEAD)" = 07a3ac562779009a2e11d0707a6f27cd6fa0e482
set -o pipefail
git -C /opt/ouf/mcp show 07a3ac562779009a2e11d0707a6f27cd6fa0e482:scripts/r4a_snapshot_mcp_runtime.py | sudo python3 - --backup-root /opt/ouf/backup
```

Annotare solo `PRIVATE_MCP_DOCKER_SNAPSHOT`. Nel laboratorio:
`/opt/ouf/backup/r4a-mcp-runtime-4i2mfgwn/container.inspect.json`.
Il file contiene valori environment e percorsi sorgente dei segreti ed è root-only 0600.

```bash
git -C /opt/ouf/mcp show 07a3ac562779009a2e11d0707a6f27cd6fa0e482:scripts/r4a_preflight_mcp_runtime.py | sudo python3 - --snapshot /opt/ouf/backup/r4a-mcp-runtime-4i2mfgwn/container.inspect.json
```

Atteso `MCP_PREFLIGHT=PASS`, `ORIGINAL_RUNNING_AND_ID_MATCH=true`, due mount read-only, zero port binding. Il lab ha osservato l'immagine originale `sha256:b909046fa0f6ecc3c09d741e7f906b6dfaacbdc6e1559e1bd6a9916e8684c465` e nessuna modifica ai container.

## 3. Preparazione privata e dry run

```bash
git -C /opt/ouf/mcp show 07a3ac562779009a2e11d0707a6f27cd6fa0e482:scripts/r4a_prepare_mcp_candidate.py | sudo python3 - --snapshot /opt/ouf/backup/r4a-mcp-runtime-4i2mfgwn/container.inspect.json
```

Annotare solo `PRIVATE_MCP_CANDIDATE`. Nel laboratorio:
`/opt/ouf/backup/r4a-mcp-runtime-4i2mfgwn/candidate-mcp-xgxnpg5w`.
La directory è root-only 0700 e contiene `mcp.env` root-only 0600. Non mostrarne il contenuto.

```bash
git -C /opt/ouf/mcp show 07a3ac562779009a2e11d0707a6f27cd6fa0e482:scripts/r4a_rollout_mcp_runtime.py | sudo python3 - --snapshot /opt/ouf/backup/r4a-mcp-runtime-4i2mfgwn/container.inspect.json --candidate /opt/ouf/backup/r4a-mcp-runtime-4i2mfgwn/candidate-mcp-xgxnpg5w
```

Atteso nel lab: `MCP_R4A_DRY_RUN=PASS`, digest candidato coincidente, `ORIGINAL_ID_MATCH=true`, `NO_CONTAINERS_CHANGED=true`. **Esito acquisito: PASS.** Se `BLOCKED`, ispezionare privatamente e correggere lo script prima di qualsiasi apply.

## 4. Staging e verifiche

**Non ancora eseguito nel lab alla data di questo documento.** Usare solo dopo CI, dry run e verifica che il container originale sia ancora attivo. Lo script salva `rollout-mcp.json` nella directory privata, disabilita il restart dell'originale, lo arresta e lo conserva come `ouf-mcp-r4a-original`, crea il candidato con ambiente e due mount invariati, quindi verifica `/health/ready`. Se il candidato fallisce tenta il rollback automatico. La procedura include un breve intervallo di indisponibilità MCP.

```bash
git -C /opt/ouf/mcp show 07a3ac562779009a2e11d0707a6f27cd6fa0e482:scripts/r4a_rollout_mcp_runtime.py | sudo python3 - --snapshot /opt/ouf/backup/r4a-mcp-runtime-4i2mfgwn/container.inspect.json --candidate /opt/ouf/backup/r4a-mcp-runtime-4i2mfgwn/candidate-mcp-xgxnpg5w --apply
sudo docker ps -a --filter name=ouf-mcp --format '{{.Names}} {{.Image}} {{.Status}}'
sudo docker exec ouf-mcp wget -q -O /dev/null http://127.0.0.1:8080/health/ready && echo MCP_R4A_READY
```

Atteso `MCP_R4A_STAGED=true`, `ouf-mcp` Up e `ouf-mcp-r4a-original` Exited. Verificare anche `ouf.system.status` con identità HUMAN, discovery tool con autenticazione, che `urban.object.search` resti assente dalla lista, e negative case senza bearer. Staging e salute non equivalgono ad acceptance della ricerca.

## 5. Rollback

Se lo staging o le prove del comportamento esistente falliscono, eseguire sul server, sempre con snapshot e candidate directory originali:

```bash
git -C /opt/ouf/mcp show 07a3ac562779009a2e11d0707a6f27cd6fa0e482:scripts/r4a_rollout_mcp_runtime.py | sudo python3 - --snapshot /opt/ouf/backup/r4a-mcp-runtime-4i2mfgwn/container.inspect.json --candidate /opt/ouf/backup/r4a-mcp-runtime-4i2mfgwn/candidate-mcp-xgxnpg5w --rollback
sudo docker exec ouf-mcp wget -q -O /dev/null http://127.0.0.1:8080/health/ready && echo MCP_ORIGINAL_READY
```

Atteso `MCP_R4A_ROLLBACK_RESTORED=true`. La procedura non tocca APISIX, UDP né la route di ricerca. Non rimuovere container originali, snapshot e candidate directory finché il collaudo non è chiuso.

## Gate successivi

Grant HUMAN specifico `urban.object.search` proposto e approvato via THS; decisione owner effettiva e receipt coerenti; chiamate positive e negative APISIX→UDP; test MCP del tool solo dopo attivazione governata e lock di release coordinato. Registrare timestamp, versioni, prove e rollback effettivo. Il client Inspector resta un ausilio di test, non parte del setup Keycloak di produzione.
