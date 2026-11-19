# Toolchain Go del server MCP

Il server richiede Go 1.27.4 per correggere GO-2026-6859 in `net/http`.
Il codice del server raggiunge la funzione vulnerabile della libreria standard (rischio R-033).
