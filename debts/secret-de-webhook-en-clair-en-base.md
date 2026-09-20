# Le secret de signature des webhooks est le troisième secret rejoué, et il est resté en clair

> **Statut :** OUVERTE · **Nature :** technique
> **Née de :** step-295 (inventaire incomplet), relevée par step-340 · **Portée par :** —

`control_plane.webhooks.secret` est un `text` en clair. C'est bien un secret **rejoué** et non
vérifié : `webhook.Sign` en a besoin en clair à chaque remise, donc le hacher le rendrait inutilisable
— la règle de `.claude/rules/go-code.md` le dit, et step-340 le redit. Mais cette même règle donne la
forme qui convient à un secret rejoué, et ce n'est pas le clair : c'est le **scellement** par
`ConfigSecrets` (`content-key-svc`, ADR-0011), colonne `*_sealed bytea` + `*_kms_key_ref`.

**Ce qu'on a fait à la place.** step-295 a scellé les deux secrets de son inventaire —
`smsc_connectors.password_hash` → `password_sealed` et
`external_billing_providers.auth_config_json` → `auth_config_sealed`. `webhooks.secret` n'y figurait
pas. step-340 a livré l'administration des webhooks en écrivant la colonne telle qu'elle est.

**Pourquoi.** L'inventaire de step-295 partait d'une chasse aux mots de passe et aux clés API ; le
secret de webhook n'est ni l'un ni l'autre, et personne ne l'écrivait encore par l'API. step-340, qui
est la première step à l'écrire, a choisi de ne pas élargir son périmètre : sceller demande une
migration, la KMS câblée dans `admin-api-svc` **et** dans `mo-dlr-router-svc` — qui doit ouvrir le
sceau à chaque remise, sur le chemin de la voie retour — plus la reprise des lignes existantes. C'est
une step, pas une ligne.

**Ce qu'il en coûte.** Une lecture de la base, une sauvegarde, un export ou un dump de
`control_plane.webhooks` rend les clés de signature de tous les clients. Qui les tient peut forger des
MO et des DLR signés valides vers les endpoints de ces clients. La surface est plus large que celle du
mot de passe de bind sortant, qui ne concerne qu'un connecteur opérateur : ici il y a une clé par
compte client et par type d'événement.

**À quoi on reconnaîtra qu'il faut la payer.** Au premier audit qui inventorie les secrets au repos —
c'est exactement ce que step-290 cherchait quand elle a ouvert step-295 — ou avant le go-live, la
passerelle détenant alors des clés de signature de clients réels. Le payer veut dire reprendre
step-295 à l'identique sur une troisième entité, avec le détail supplémentaire que le déchiffrement
est sur le chemin critique de la remise et non au démarrage d'un pod.

Sources : `db/schema_passerelle_sms.sql:511` (`secret text NOT NULL`),
`internal/webhook/webhook.go:224` (`Sign` le rejoue en clair),
`tasks-done/step-295.md` (l'inventaire qui l'a manqué).
