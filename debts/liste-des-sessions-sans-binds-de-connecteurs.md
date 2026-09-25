# La liste des sessions ignore les binds sortants des connecteurs

> **Statut :** OUVERTE · **Nature :** produit
> **Née de :** step-360 (design arrêté, point 5) · **Portée par :** —

**Ce qu'on a fait à la place.** `list-sessions` ne rend que les binds clients (ESME) du registre de
sessions ; le filtre `connectorId` répond 422 et renvoie vers `get-connector-status`
(`internal/adminapi/sessions.go:114`). Le contrat déclare pourtant `direction: smsc` et
`connector_id` sur `Session`, et le paramètre `connectorId` (`api/openapi-admin.yaml`,
`list-sessions`).

**Pourquoi.** Les binds sortants vivent dans `connector-pool-svc`, sans registre inter-pods, sans UUID
de session ni `connected_at` : les champs requis de `Session` n'ont pas de source. Une page vide
affirmerait qu'un connecteur n'a aucun bind.

**Ce qu'il en coûte.** Le tableau de bord ne peut pas montrer toutes les sessions sur une seule vue ; un
opérateur qui filtre par connecteur reçoit une erreur, et `direction`/`connector_id` restent une
promesse du contrat que rien ne tient.

**À quoi on reconnaîtra qu'il faut la payer.** Une demande du tableau de bord pour une vue unifiée des
sessions, ou une déconnexion forcée d'un bind sortant précis (aujourd'hui seul `rebind-connector`
agit, sur tout le pool).
