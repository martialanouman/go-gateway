# Aucune jauge `smpp_sessions_active` sur le flux de métriques

> **Statut :** OUVERTE · **Nature :** produit
> **Née de :** step-403 (design arrêté, point E) · **Portée par :** —

**Ce qu'on a fait à la place.** Le nombre de binds SMPP ouverts se lit à la demande dans
`get-metrics-summary` (`active_sessions`, RPC `CountSessions`), pas sur `stream-metrics`.

**Pourquoi.** session-manager n'a aucun émetteur `metricstream` ni producteur `metrics.stream`
(`cmd/session-manager-svc`). L'y ajouter est un câblage de service entier pour une tuile que la synthèse
sert déjà, interrogée toutes les quelques secondes.

**Ce qu'il en coûte.** Le tableau de bord sonde la synthèse au lieu de recevoir le chiffre, et le voit avec
le retard de son intervalle de sondage.

**À quoi on reconnaîtra qu'il faut la payer.** Le tableau de bord branche le flux pour l'écran Trafic et
veut la tuile « sessions » sans sondage. La forme attendue : une jauge par instance de session-manager, ou
une jauge par pod smpp-server-svc de ses binds locaux, à sommer entre instances (règle de `MetricSnapshot`).

Sources : `internal/adminapi/metrics.go:140` · `internal/session/registry.go:278`
