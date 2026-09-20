# step-295 — Deux secrets stockés sous une forme qui ne sert pas leur usage

> **Jalon :** Dette ouverte par step-290d · **Statut :** FAITE
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

Voie **1** : les deux secrets sont **scellés par la KMS de `content-key-svc`**. Arbitrage Fable du
2026-09-20, en deux tours. La spec ne tranchait pas — elle ne parle du stockage des secrets qu'à la
l.773, et uniquement du cas **entrant**, celui que cette fiche ne rouvre pas.

**La décision, ses deux options écartées et les noms de colonnes sont dans ADR-0016.** Ils ne sont pas
recopiés ici : deux copies d'un même arbitrage divergent, et c'est l'ADR qui fait autorité.

Ce que la fiche garde, parce que l'ADR ne le porte pas :

### Le contrat Admin ne bouge pas — et c'est un revirement

Le premier tour d'arbitrage proposait de retirer `auth_config_json` des réponses (write-only). **Infirmé
au second tour**, sur deux faits qui manquaient :

- `maskedAuthConfig()` renvoie une **constante** — elle n'a jamais lu la vraie valeur. Une fois le secret
  scellé, ce code ne change pas d'une ligne et n'appelle jamais `Open`. Le masquage révèle donc
  exactement autant que le write-only : rien. « Plus fort que le masquage » était faux.
- Le retrait serait une **rupture majeure** (4.2.0 → 5.0.0), payée par le tableau de bord, plus une
  sémantique de PATCH à faire changer chez le client — pour supprimer une douzaine de lignes.

Seule la description `password` change (« stored hashed » mentait) : bump **correctif** 4.2.1.

### Périmètre — le pool garde `CONNECTOR_PASSWORD`, et la DoD exige que ce soit écrit

`connector-pool-svc` ne lit **aucune** colonne de bind et **n'a aucun client Postgres**. Câbler le seul
mot de passe lui ajouterait cette dépendance pour une colonne et produirait un bind **hybride** — adresse
et `system_id` depuis l'environnement, mot de passe depuis la base — deux sources de vérité pour une même
session. `debts/ancre-de-confiance-par-connecteur.md` a déjà écrit la règle : les douze champs migreront
ensemble. Le mot de passe est le **treizième**, et l'addendum de cette PR l'y inscrit.

**Défaut qui survit à cette step**, et que la fiche ne nommait pas : rien ne relie la colonne à
`CONNECTOR_PASSWORD`. Une rotation par l'Admin API répond 200 et le bind n'en sait rien. La colonne est
devenue utilisable ; la rotation n'est pas devenue effective.

### Ce que la revue a changé au design

Trois axes en lecture seule ont tourné sur le diff. Deux constats ont modifié la conception elle-même,
pas seulement le code :

1. **La séparation des domaines manquait.** `ConfigSecrets` et `ContentKeys` partagent la même KMS, et
   `LocalKMS` ne lie que son `KeyRef` en AAD : tout ce qui est scellé sous la clé maître vivait dans un
   seul espace. `Open` rendait donc 32 octets de DEK de contenu en clair quand on lui passait un
   `content_keys.wrapped_key`, et `Seal` fabriquait un `wrapped_key` valide à partir d'octets choisis.
   L'isolation qu'annonçait l'ADR était un **nom de service**, pas une frontière. Corrigé par un tag de
   domaine de 32 octets dans le clair scellé, plus un garde de taille au déballage de `contentkeys`.
2. **L'allowlist admet un binaire, pas une méthode.** Enregistrer `ConfigSecrets` sur le listener partagé
   donnait immédiatement à `router-svc` de quoi ouvrir tous les mots de passe de bind. Corrigé par un
   intercepteur d'autorisation par méthode, dont la liste d'appelants n'est **pas** de la configuration.

ADR-0016 a été amendé en conséquence.

### Ce que la PR livre, et ce qu'elle prouve

1. `ConfigSecrets` servi par `content-key-svc`, avec autorisation par méthode.
2. L'Admin API scelle à l'écriture des deux entités, via le vrai client gRPC.
3. **Aller-retour utilisable, de bout en bout** : écrit par l'API HTTP → relu de la colonne → ouvert →
   égal à l'entrée. C'est ce que `password_hash` rendait impossible.
4. **Jamais rendu** : ni le clair, ni les octets scellés, ni la référence de clé, en lecture comme en
   création.
5. `.claude/rules/go-code.md` distingue désormais un secret qu'on **vérifie** d'un secret qu'on
   **rejoue** — l'absence de cette distinction est la cause racine des deux défauts.

## Definition of Done

- [x] La voie est tranchée et écrite dans un ADR — **ADR-0016**, qui étend ADR-0011 plutôt que de le
      contredire : la surface de *dépendances* de `content-key-svc` ne change pas, celle de ses *appelants*
      oui, et c'est gardé par méthode.
- [x] Les deux secrets suivent cette voie, migration `0017` comprise, avec le renommage :
      `password_hash` → `password_sealed` + `password_kms_key_ref`, `auth_config_json` →
      `auth_config_sealed` + `auth_config_kms_key_ref`. `up`/`down`/`up` vérifié sur PostgreSQL 18.
- [x] `connector-pool-svc` garde `CONNECTOR_PASSWORD`, et la raison est écrite — ici (§Périmètre), dans
      son propre commentaire de paquet, et dans `debts/ancre-de-confiance-par-connecteur.md` : il ne lit
      **aucune** colonne de bind et n'a pas de client Postgres ; câbler le seul mot de passe ferait un
      bind à deux sources de vérité.
- [x] Un test prouve les deux moitiés, et sur la chaîne entière :
      `TestAConnectorPasswordWrittenByTheAdminAPIOpensAgainFromPostgres` écrit par l'API HTTP, relit la
      colonne et l'**ouvre** ; `TestReadingAConnectorNeverReturnsTheSealedPassword` et
      `TestProviderAuthConfigMaskedOnRead` prouvent qu'il n'est jamais rendu — ni en clair, ni scellé, ni
      en base64, ni sa référence de clé.

**Ce que la step a fermé en plus, parce que la revue l'a trouvé :** `ConfigSecrets.Open` rendait la clé de
contenu d'un client en clair, et `Seal` permettait d'en forger une — les deux domaines partageaient un
espace de chiffrés indistinguables. Et enregistrer le service sur le listener partagé avait donné à
`router-svc` de quoi ouvrir tous les mots de passe de bind. Aucun des deux n'était dans la fiche.

## Hors périmètre

Le transport (TLS vers le SMSC, mTLS) → step-300.
