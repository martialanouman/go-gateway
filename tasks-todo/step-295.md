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

`auth_config_json jsonb NOT NULL DEFAULT '{}'::jsonb` porte les identifiants d'appel au fournisseur de
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

## Design arrêté

Voie **1** — les deux secrets sont **scellés par la KMS de `content-key-svc`**. Arbitrage Fable du
2026-09-20, en deux tours (le second a infirmé un point du premier, voir §4).

La spec ne tranche pas : elle ne parle du stockage des secrets qu'à la l.773, et uniquement du cas
**entrant** (« seul le hash est stocké »), celui que cette fiche ne rouvre pas.

### 1. Pourquoi la voie 1, et pas les deux autres

L'argument d'ADR-0011 est « la KEK vit dans le moins de process possible ». Il porte sur **qui détient
la clé maître**, pas sur **qui peut demander un déchiffrement** — et c'est ce qui départage :

- **Voie 3 (clé de service dédiée) est la pire sous le critère de l'ADR lui-même.** Une **seconde** clé
  maître, qui vivrait dans `connector-pool-svc` et `billing-svc` : deux process du chemin chaud, avec
  Redis, Kafka et du HTTP sortant. C'est exactement le « rayon d'explosion » (ADR-0011 §Context, pt. 2)
  que la sortie de `billing-svc` avait fui. Deux KEK dont une dans un process large, ce n'est pas
  « moins de process », c'est le double.
- **Voie 1 n'ajoute aucune exposition marginale.** Un appelant d'`Open` reçoit *un* mot de passe en
  clair — celui qu'il va de toute façon écrire en clair dans la PDU `bind_transceiver` (SMPP v3.4
  §4.1.1). La KEK, elle, ne quitte jamais `content-key-svc`. Le précédent existe et il est documenté :
  `GetContentEncryptionKey` rend déjà des DEK en clair au plan de données sur ce même canal gardé.
- **Voie 2 (référence à un secret externe) casse la source de vérité.** Le dépôt a une Admin API de
  connecteurs (création, PATCH, `parked`) : un connecteur créé par l'API doit être fonctionnel sans
  `kubectl`. La voie 2 est juste pour un secret **statique et global** (la KEK, les certificats TLS —
  c'est déjà `gateway-secrets`), pas pour une donnée **par entité, créée à l'exécution**. Et
  `debts/secrets-tls-non-provisionnes-par-le-depot.md` montre que le hors-bande est déjà le mode de
  panne le plus muet du dépôt.

Ce que la voie 1 coûte réellement : le **cercle des appelants** s'élargira (pool, billing). Ce coût
n'est pas payé par cette step (§5), et il est borné par le découpage du §2.

**ADR-0016** — « `content-key-svc` scelle aussi les secrets de configuration rejoués ». Il amende
ADR-0011 sur un point précis : la surface de **dépendances** ne change pas (Postgres + gRPC, toujours
aucun client sortant) ; c'est la surface d'**appelants** qui s'élargit, et elle se garde par service.

### 2. Un second service gRPC, sans état, dans le même binaire

`api/proto/configsecrets.proto`, package `configsecrets` :

```
service ConfigSecrets {
  rpc Seal(SealRequest) returns (SealResponse);   // plaintext        -> {kms_key_ref, sealed}
  rpc Open(OpenRequest) returns (OpenResponse);   // {kms_key_ref, sealed} -> plaintext
}
```

**Pas une extension de `ContentKeys`.** `ContentKeys` est scopé client et **stocké** (store, cycle de
vie `active`/`retired`/`destroyed`, crypto-shred) ; `ConfigSecrets` est **sans état** — ni store, ni
`customer_id`, ni rotation — et sert d'autres appelants. C'est cette séparation qui permettra, le jour
du câblage du pool, de l'autoriser sur `ConfigSecrets/Open` **sans** lui ouvrir
`ContentKeys/GetContentEncryptionKey`.

**`WrapDataKey` directement sur les octets du secret, pas d'enveloppe par secret.** Un mot de passe
SMPP fait ≤ 8 octets (`internal/smpp/pdu.go:74` borne le décodage à 9) ; une DEK en ferait 32 —
l'enveloppe coûterait 4× le secret sans rien protéger de plus. La borne de nonce GCM (~2³² scellements
par clé) qui justifie le HKDF-par-message de `SealBody` est hors d'atteinte pour des écritures de
configuration. Le nonce est aléatoire à chaque appel (`internal/content/aead.go:45`), donc deux
connecteurs au même mot de passe ne produisent pas le même chiffré. `KeyRef()` est persisté à côté du
chiffré, comme `content_keys.kms_key_ref`, pour qu'une rotation de KEK reste possible.

Pas d'AAD liant l'identifiant de ligne : permuter deux chiffrés entre lignes exige d'écrire en base, et
qui écrit en base peut aussi bien changer le `host` du connecteur. L'AAD n'achèterait rien. (`LocalKMS`
lie déjà `KeyRef` en AAD, ce qui donne la séparation de domaine.)

### 3. Colonnes — le nom suit la décision

| Table | Avant | Après |
|---|---|---|
| `smsc_connectors` | `password_hash text NOT NULL` | `password_sealed bytea NOT NULL` + `password_kms_key_ref text NOT NULL` |
| `external_billing_providers` | `auth_config_json jsonb NOT NULL DEFAULT '{}'` | `auth_config_sealed bytea NOT NULL` + `auth_config_kms_key_ref text NOT NULL` |

`_sealed` plutôt que `_enc` : le mot fait écho à `SealContent`/`OpenContent` déjà dans
`internal/content`, et il ne peut plus se confondre avec les `*_hash` **entrants** de `credentials`,
qui restent hachés à juste titre.

Le document d'authentification est scellé **en bloc**, pas champ par champ : le chiffrement par champ
exigerait une politique « quels champs sont secrets » par type de fournisseur (bearer, basic, HMAC…) à
maintenir, alors que **personne ne requête l'intérieur** de ce JSON — vérifié, seul l'Admin API le lit,
pour le masquer.

Pas de `DEFAULT` sur les colonnes scellées : le scellé de `{}` n'est pas une constante (nonce
aléatoire). L'Admin API scelle `{}` quand rien n'est fourni.

**Migration `0017_*` non convertissante, avec garde explicite.** Un hash argon2id ne se déchiffre pas :
il n'y a rien à convertir, et le dépôt n'a jamais été déployé. La migration refuse de s'appliquer sur
une table non vide, avec un message qui dit de recréer les connecteurs et les fournisseurs — plutôt
qu'un `NOT NULL` violé dont le message ne dirait rien. Schéma **et** migration dans la même PR
(`.claude/rules/db-schema.md`).

### 4. Le contrat Admin ne bouge pas — et c'est un revirement

Le premier tour d'arbitrage proposait de retirer `auth_config_json` des réponses (write-only). **Infirmé
au second tour**, sur deux faits qui manquaient :

- `maskedAuthConfig()` (`internal/adminapi/billing_admin.go:446`) renvoie la **constante**
  `{"masked": true}` — elle n'a jamais lu la vraie valeur. Une fois le secret scellé, ce code ne change
  pas d'une ligne et n'appelle jamais `Open`. Le masquage révèle donc exactement autant que le
  write-only : rien. « Plus fort que le masquage » était faux.
- Le retrait serait une **rupture majeure** de `api/openapi-admin.yaml:2646` (4.2.0 → 5.0.0), payée par
  le tableau de bord, plus une sémantique de PATCH à faire changer chez le client — pour supprimer une
  douzaine de lignes. `isMaskedSentinel()` reste la plus petite garde possible du PATCH partiel.

Donc : `password` reste un string write-only, `auth_config_json` reste un objet en écriture et
`{"masked":true}` en lecture. **Aucun bump de `api/package.json`.** Seule la forme de stockage bouge.

### 5. Périmètre — le pool garde `CONNECTOR_PASSWORD`, et voici pourquoi

La DoD exige que ce choix soit écrit. Il l'est ici :

`connector-pool-svc` ne lit **aucune** colonne de bind de `smsc_connectors` et **n'a aucun client
Postgres** pour le faire (`connectorConfigSource` ne remonte que `bind_pool_size` et la politique de
reconnexion). Câbler le seul mot de passe lui ajouterait une dépendance Postgres pour une colonne, et
produirait un bind **hybride** — host/port/system_id depuis l'environnement, mot de passe depuis la
base — soit deux sources de vérité pour une même session.
`debts/ancre-de-confiance-par-connecteur.md` a déjà écrit la règle : « le jour où le pool lira sa
configuration de connecteur dans la base, les douze champs migreront ensemble ». Le mot de passe est le
**treizième**. Ce n'est pas cette step ; c'est la dette du pool, et cette PR l'y inscrit.

**Défaut que la fiche ne nommait pas, et qui survit à cette step :** rien ne relie
`smsc_connectors.password_hash` à `CONNECTOR_PASSWORD`. Un opérateur fait tourner le mot de passe par
l'Admin API, reçoit 200, et le bind n'en sait rien — le symptôme exact que la dette du connecteur
décrit pour `tls_config_json` (« une surface qui répond 200 à un réglage sans effet »), cette fois sur
un secret. Cette PR ne le corrige pas ; elle l'écrit dans la fiche de dette, qui porte déjà la même
cause.

### 6. Ce que la PR livre, et ce qu'elle prouve

1. `ConfigSecrets` servi par `content-key-svc`, sur le listener existant (`admin-api-svc` est déjà dans
   l'allowlist SAN — rien à ouvrir).
2. L'Admin API scelle à l'écriture des deux entités, via le **vrai** client gRPC (serveur en process
   avec `LocalKMS`), pas un double.
3. **Aller-retour utilisable** : écrire par l'Admin API → relire la ligne → `Open` → octet pour octet
   égal à l'entrée. C'est ce que la colonne `password_hash` rendait impossible.
4. **Jamais rendu** : les octets du secret n'apparaissent ni dans la réponse HTTP, ni dans les
   journaux. Pour `auth_config_json` la moitié « jamais rendu » est déjà tenue
   (`billing_admin_test.go:217`) ; c'est la moitié « relu utilisable » qui manquait aux deux.
5. `.claude/rules/go-code.md:20` amendée : la règle « secrets stockés en hash » ne distingue pas
   **vérifier** de **rejouer**, et c'est cette absence de distinction qui a produit les deux défauts.
6. Le commentaire périmé du pool (« M2 has no config-sync » — `config-sync` existe depuis step-105) est
   corrigé au passage.

## Definition of Done

- [ ] La voie est tranchée et écrite dans un ADR (ou un addendum à ADR-0011 si la KMS est retenue).
- [ ] Les deux secrets suivent cette voie, migration comprise, avec le renommage de colonne qu'elle impose.
- [ ] `connector-pool-svc` lit le mot de passe du plan de contrôle, ou la fiche dit explicitement pourquoi
      l'environnement reste la source jusqu'à M3+.
- [ ] Un test prouve qu'un secret écrit est relu **utilisable**, et qu'il n'est jamais rendu par l'API.

## Hors périmètre

Le transport (TLS vers le SMSC, mTLS) → step-300.
