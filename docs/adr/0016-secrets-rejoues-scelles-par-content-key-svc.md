# ADR-0016 : Les secrets qu'on rejoue sont scellés par `content-key-svc` (étend 0011)

**Status:** Accepted
**Date:** 2026-09-20
**Deciders:** Équipe plateforme
**Réf spec:** §6.8 ; §6.10 ; plan §1.9 ; ADR-0011 ; step-290d → step-295

## Context

Le dépôt traite tous ses secrets de la même façon — il les hache — parce que la règle écrite
(plan §1.9, `.claude/rules/go-code.md`) dit « les secrets sont stockés en hash, révélés une seule fois ».
Cette règle est juste, mais elle ne vaut que pour les secrets que la passerelle **vérifie**. Elle a été
appliquée à deux secrets que la passerelle doit **rejouer**, et dans ce cas elle produit l'inverse de la
sécurité visée :

1. **`smsc_connectors.password_hash`** est le mot de passe d'un bind SMPP **sortant**. SMPP v3.4 §4.1.1
   met ce mot de passe **en clair dans la PDU** `bind_transceiver` : un hash argon2id ne s'y substitue
   pas. La colonne est donc inutilisable pour son unique raison d'être. `connector-pool-svc` le
   contourne en lisant `CONNECTOR_PASSWORD` dans l'environnement — et le dit dans son en-tête. Résultat :
   l'Admin API accepte une rotation, répond 200, et le bind n'en sait rien.
2. **`external_billing_providers.auth_config_json`** porte les identifiants d'appel au fournisseur de
   facturation externe (§6.10). Il est **en clair** dans Postgres, donc dans les sauvegardes et dans
   toute réplique. L'Admin API le masque en lecture, ce qui protège la sortie HTTP et rien d'autre.

Les deux ont le même besoin — un secret **réversible**, parce qu'il repart vers un tiers — et la même
cause : une règle écrite pour le cas entrant, appliquée au cas sortant. Les trancher séparément, ce
serait trancher deux fois.

Le cas entrant, lui, reste correct et n'est pas rouvert : `credentials.password_hash` et la clé API sont
hachés parce que la passerelle les **compare**, jamais ne les rejoue.

## Decision

**Un secret de configuration qu'il faut rejouer est stocké scellé, et `content-key-svc` — déjà seul
détenteur de la clé maître (ADR-0011) — est ce qui le scelle et le descelle.**

- Un **second service gRPC, sans état**, dans le binaire existant :
  `api/proto/configsecrets.proto`, package `configsecrets`.
  `Seal(plaintext) → {kms_key_ref, sealed}` · `Open(kms_key_ref, sealed) → plaintext`.
- **Pas une extension de `ContentKeys`.** Celui-ci est scopé client et *stocké* (cycle de vie
  `active`/`retired`/`destroyed`, crypto-shred) ; `ConfigSecrets` n'a ni store, ni `customer_id`, ni
  rotation.
- **Deux séparations sont nécessaires, et un service distinct n'en fournit AUCUNE par lui-même.** La
  revue de conception de cette step l'a établi en exhibant les deux trous ; ils sont refermés ici, et la
  formulation initiale de cet ADR — « la séparation est ce qui permettra d'autoriser un appelant sur
  `Open` sans lui ouvrir les clés de contenu » — était fausse telle quelle :
  - **Séparation cryptographique.** Les deux services partagent la même KMS, et `LocalKMS` ne lie que son
    `KeyRef` en AAD : tout ce qui est scellé sous la clé maître vit dans un seul espace. Sans discriminant,
    `Open` rendait la DEK d'un client à qui lui passait un `content_keys.wrapped_key`, et `Seal`
    fabriquait un `wrapped_key` valide à partir d'octets choisis. **Un tag de domaine** est préfixé au
    clair avant scellement et exigé au déballage ; il fait exactement 32 octets — la taille d'une DEK — et
    `Seal` refuse un secret vide, donc ce service ne peut pas produire une clé de données. `contentkeys`
    refuse symétriquement toute clé déballée qui n'en a pas la taille.
  - **Séparation d'autorisation.** L'allowlist mTLS est vérifiée au **handshake** : elle admet un binaire
    et ne voit jamais la méthode. Enregistrer `ConfigSecrets` sur ce listener donnait donc immédiatement à
    `router-svc` de quoi ouvrir tous les mots de passe de bind. **Un intercepteur** autorise par méthode.
    Sa liste d'appelants n'est pas de la configuration : quel service peut sceller un secret du plan de
    contrôle est une propriété de ce service, et une variable d'environnement l'élargirait par accident.

  Le service distinct garde sa valeur — il rend le filtrage lisible et la cohésion juste — mais c'est
  l'intercepteur qui autorise, et le tag qui sépare.
- **`WrapDataKey` directement sur les octets du secret**, sans enveloppe par secret. Un mot de passe SMPP
  fait au plus 8 octets, une DEK en ferait 32 : l'enveloppe coûterait quatre fois le secret sans rien
  protéger de plus. La borne de nonce GCM (~2³² scellements par clé) qui justifie le HKDF-par-message de
  `SealBody` est hors d'atteinte pour des écritures de configuration. Le nonce étant tiré à chaque appel,
  deux connecteurs au même mot de passe ne produisent pas le même chiffré.
- **`kms_key_ref` est persisté à côté du chiffré**, comme `content_keys.kms_key_ref`, pour qu'une
  rotation de clé maître reste possible sans deviner sous quelle KEK une ligne a été scellée.
- **Les colonnes sont renommées**, parce que l'ancien nom mentait :

  | Table | Avant | Après |
  |---|---|---|
  | `smsc_connectors` | `password_hash text` | `password_sealed bytea` + `password_kms_key_ref text` |
  | `external_billing_providers` | `auth_config_json jsonb` | `auth_config_sealed bytea` + `auth_config_kms_key_ref text` |

  `_sealed` fait écho à `SealContent`/`OpenContent` (`internal/content`) et ne peut plus se confondre
  avec les `*_hash` entrants, qui restent des hash.

### Ce que cet ADR amende dans ADR-0011

ADR-0011 justifie `content-key-svc` par « la surface la plus étroite possible : Postgres + gRPC, aucun
client sortant ». **Cette phrase reste vraie mot pour mot** : sceller un mot de passe n'ajoute ni
dépendance, ni client sortant, ni port. Ce qui change est ailleurs, et il faut le nommer : la **surface
d'appelants** s'élargira le jour où `connector-pool-svc` et `billing-svc` devront desceller leur secret.

C'est le coût réel de cette décision, et il est borné par deux choses : le service distinct ci-dessus, et
le fait qu'un appelant d'`Open` reçoit *un* secret — celui qu'il va de toute façon mettre en clair sur le
fil — jamais la clé maître.

## Options Considered

### Option A (retenue) : scellés par la KMS de `content-key-svc`
**Pros :** aucune seconde clé maître ; aucune dépendance nouvelle pour le dépositaire ; le précédent
existe et il est documenté (`GetContentEncryptionKey` rend déjà du matériel de clé en clair sur ce canal,
sous mTLS et allowlist par SAN) ; le plan de contrôle reste la source de vérité du connecteur.
**Cons :** le cercle des appelants du détenteur de la KEK s'élargit. Et — c'est le coût que cette
décision a d'abord sous-estimé — partager la KMS exige de séparer explicitement les deux domaines, dans
les deux sens, parce que rien dans le chiffrement ne les distingue autrement.

### Option B : référence à un secret externe (`Secret` Kubernetes, coffre)
La colonne ne porte qu'un nom, le pod monte la valeur ; aucun secret ne touche Postgres.
**Cons :** le plan de contrôle cesse d'être la source de vérité. Le dépôt a une Admin API de connecteurs
(création, PATCH, `parked`) : un connecteur créé par l'API doit être fonctionnel sans `kubectl`. Cette
option est juste pour un secret **statique et global** — la KEK et les certificats TLS le sont déjà, dans
`gateway-secrets` — pas pour une donnée **par entité, créée à l'exécution**. Et
`debts/secrets-tls-non-provisionnes-par-le-depot.md` documente déjà le hors-bande comme le mode de panne
le plus muet du dépôt : un volume absent laisse un pod en `ContainerCreating` sans limite de temps et
sans une ligne de journal.

### Option C : une clé de service dédiée, indépendante de la KMS de contenu
**Cons :** c'est la pire sous le critère d'ADR-0011 lui-même. Une **seconde** clé maître, qui vivrait
dans `connector-pool-svc` et `billing-svc` — deux process du chemin chaud, avec Redis, Kafka et du HTTP
sortant. C'est exactement le « rayon d'explosion » que la sortie de `billing-svc` avait fui. Deux KEK dont
une dans un process large, ce n'est pas « moins de process », c'est le double.

## Consequences

- **Plus facile :** faire tourner un mot de passe de connecteur par l'Admin API et que ça veuille dire
  quelque chose ; répondre « aucun secret rejouable n'est en clair au repos » sans exception à énoncer.
- **Plus difficile / limites :**
  - Le jour où `connector-pool-svc` descellera son mot de passe, il rejoint `configSecretsCallers`
    (`cmd/content-key-svc/authz.go`) et **rien d'autre** : l'allowlist TLS reste par binaire, c'est
    l'intercepteur qui distingue les méthodes.
  - `admin-api-svc` ne peut plus créer ni modifier un connecteur si `content-key-svc` est indisponible.
    C'est une opération de configuration, pas un chemin chaud ; l'indisponibilité est visible et bornée.
  - Une rotation de clé maître doit desceller et resceller les lignes existantes. `kms_key_ref` rend
    l'opération possible ; elle n'est pas outillée, et aucune rotation n'est prévue avant le go-live.
- **Inchangé :** le contrat Admin (`password` reste write-only, `auth_config_json` reste masqué en
  lecture par une constante qui n'a jamais lu la valeur) ; le cycle de vie des clés de contenu ; le
  schéma `content_keys` ; la crypto. Le fournisseur KMS réel (AWS/GCP/Vault) reste hors périmètre,
  derrière `content.KMS`.
- **Ce que cet ADR ne fait pas :** `connector-pool-svc` continue de lire `CONNECTOR_PASSWORD` dans
  l'environnement. Le pool ne lit aujourd'hui **aucune** colonne de bind et n'a aucun client Postgres ;
  ne câbler que le mot de passe produirait un bind hybride, à deux sources de vérité pour une même
  session. Le mot de passe rejoint donc la dette qui porte déjà les douze autres champs
  (`debts/ancre-de-confiance-par-connecteur.md`) : ils migreront ensemble.
- **Traçabilité :** `tasks-done/step-295.md` ; `.claude/rules/go-code.md` amendée pour distinguer un
  secret qu'on **vérifie** d'un secret qu'on **rejoue** — c'est l'absence de cette distinction qui a
  produit les deux défauts.
