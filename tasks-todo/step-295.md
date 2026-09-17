# step-295 — Deux secrets stockés sous une forme qui ne sert pas leur usage

> **Jalon :** Dette ouverte par step-290d · **Statut :** À FAIRE
> **Dépend de :** — · **Bloque :** —

## Pourquoi cette fiche existe

step-290 devait vérifier « qu'aucun secret n'est en clair (hash pour mots de passe bind & clés API,
§1.9) ». L'inventaire a trouvé l'inverse de ce que la fiche cherchait : deux secrets dont la **forme de
stockage** ne correspond pas à ce qu'on doit en faire. Aucun des deux ne se corrige par un hash de plus.

## Constat 1 — `smsc_connectors.password_hash` est inutilisable pour un bind sortant

`db/schema_passerelle_sms.sql` déclare `password_hash text NOT NULL`. Un bind SMPP **sortant** envoie le
mot de passe **en clair** dans la PDU `bind_transceiver` (SMPP v3.4 §4.1.1) : un hash ne se dé-hache pas.
La colonne ne peut donc pas servir à ce pour quoi elle existe.

C'est pour cela que `connector-pool-svc` lit `CONNECTOR_PASSWORD` dans l'environnement, et le dit :
« the outbound password cannot be recovered from its stored hash, and M2 has no config-sync »
(`cmd/connector-pool-svc/main.go`). Le contournement tient tant qu'un pod sert **un** connecteur. Il ne
tient plus dès que le plan de contrôle source les connecteurs (M3+), ce que le bloc d'environnement
annonce lui-même comme provisoire.

**Ce qu'il faut trancher :** un secret sortant doit être **rejouable**, donc réversible. Trois voies,
à départager :

1. **Chiffré par la KMS de `content-key-svc`** (ADR-0011). Le dépositaire de la clé maître existe déjà,
   et son périmètre est volontairement minimal. Lui confier un second usage change ce périmètre.
2. **Référence à un secret externe** (`Secret` Kubernetes, coffre) : la colonne ne porte qu'un nom, et le
   pod monte la valeur. Le plan de contrôle cesse d'être la source de vérité du connecteur.
3. **Chiffré par une clé de service dédiée**, indépendante de la KMS de contenu.

Le nom de la colonne devra suivre la décision : `password_hash` ment aujourd'hui.

## Constat 2 — `external_billing_providers.auth_config_json` est stocké en clair

`auth_config_json jsonb NOT NULL DEFAULT '{}'` porte les identifiants d'appel au fournisseur de
facturation externe (§6.10). L'Admin API le **masque en lecture** (`internal/adminapi/billing_admin.go`,
`maskedAuthConfig()`), ce qui protège la sortie HTTP — et rien d'autre : la valeur est en clair dans
Postgres, donc dans les sauvegardes et dans toute réplique.

Même besoin que le constat 1 — un secret qu'il faut **rejouer** vers un tiers — donc même décision, et
c'est la raison pour laquelle les deux sont dans la même fiche. Les trancher séparément, c'est trancher
deux fois.

## Ce que cette fiche N'EST PAS

Elle ne rouvre pas §1.9. Le mot de passe de bind **entrant** (`credentials.password_hash`) et la clé API
sont hachés, et c'est correct : la passerelle les **vérifie**, elle ne les rejoue jamais. Les deux
secrets ci-dessus sont dans la situation inverse.

## Definition of Done

- [ ] La voie est tranchée et écrite dans un ADR (ou un addendum à ADR-0011 si la KMS est retenue).
- [ ] Les deux secrets suivent cette voie, migration comprise, avec le renommage de colonne qu'elle impose.
- [ ] `connector-pool-svc` lit le mot de passe du plan de contrôle, ou la fiche dit explicitement pourquoi
      l'environnement reste la source jusqu'à M3+.
- [ ] Un test prouve qu'un secret écrit est relu **utilisable**, et qu'il n'est jamais rendu par l'API.

## Hors périmètre

Le transport (TLS vers le SMSC, mTLS) → step-300.
