# Les refus à l'admission ne sont comptés nulle part

> **Statut :** OUVERTE · **Nature :** produit
> **Née de :** step-403 (design arrêté, point C) · **Portée par :** —

**Ce qu'on a fait à la place.** `get-metrics-summary` n'expose aucun refus d'admission par canal. Un
submit refusé avant l'ACK (authentification, débit `429` / `ESME_RTHROTTLED`, canal désactivé) n'écrit
aucune ligne CDR, et aucun compteur ne le voit : `rejected_total{code}` et `submit_rejected_total` comptent
les rejets du pipeline et du SMSC, après l'ACK.

**Pourquoi.** La synthèse lit ClickHouse, et le refus d'admission n'y laisse rien par construction : il
survient avant la durabilité qui écrit la ligne accepted (`internal/ingest/ingest.go`, `Accept`). Le
compter demande un compteur dans rest-api-svc et smpp-server-svc, qui n'enregistrent pas le catalogue, et
une lecture que la synthèse n'a pas.

**Ce qu'il en coûte.** Un client refusé en boucle (mauvaise clé, débit dépassé) est invisible au tableau de
bord : l'écran Trafic montre un débit entrant normal pendant qu'il rejette.

**À quoi on reconnaîtra qu'il faut la payer.** Le widget « refus à l'admission » du tableau de bord, ou un
incident où un client se plaint de refus que l'exploitation ne voit pas.

Sources : `internal/ingest/ingest.go:63` · `internal/router/router.go:260` · `internal/connectorpool/metrics.go:26`
