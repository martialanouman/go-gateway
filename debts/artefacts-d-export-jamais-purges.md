# Les artefacts d'export de messages ne sont jamais purgés

> **Statut :** OUVERTE · **Nature :** produit
> **Née de :** step-187 (`tasks-done/step-187.md:72`) · **Portée par :** —

Le job pose un `ExpiresAt` à +24 h et rien ne supprime le fichier. La fiche l'assume : « **dette
explicite**, pas un oubli : le job annonce quand l'artefact n'est plus garanti, rien ne le supprime
encore » — la suppression « appartient à l'infra, comme la rétention froide ».

**Ce qu'il en coûte.** Non écrite. En pratique : des exports de dizaines de milliers de lignes de CDR,
masqués par rôle mais bien réels, s'accumulent sur `EXPORT_DIR` et **survivent à un effacement
RGPD** — exactement la question que step-297 pose pour `audit_log`, sans nommer les exports.

**À quoi on reconnaîtra qu'il faut la payer.** La première demande d'effacement qui doit couvrir les
exports, ou un disque de pod qui se remplit.

Source : `internal/adminapi/messages_export.go:167`
