# ADR-0008 : Stockage de contenu chiffré par clé client, jamais loggé

**Status:** Accepted
**Date:** 2026-07-14
**Deciders:** Équipe plateforme
**Réf spec:** §6.11, §6.14.4, §6.23, §7

## Context

Le corps d'un SMS est la donnée la plus sensible (PII, OTP, bancaire). Faut-il le stocker ? Où ? Sous quelle protection ? Et comment satisfaire l'effacement RGPD à la demande sans réécrire des volumes massifs de CDR ?

## Decision

Le stockage du corps est une **décision configurable** (`content_storage` : `off` / `stored_plaintext` déconseillé / `stored_encrypted` recommandé), défaut plateforme surchargeable par client. Quand stocké et chiffré, le corps l'est avec une **clé par client** (enveloppe KMS, `content_keys`), lisible uniquement depuis le tableau de bord sous permission `content:read` (auditée). **Invariant absolu et testable :** le corps n'apparaît **jamais** dans un log ni un span, sous aucune politique. L'effacement RGPD d'un client se fait par **crypto-shred** (destruction de la clé), sans réécrire le CDR.

## Options Considered

### Option A : configurable, chiffré par clé client, jamais loggé (retenue)
| Dimension | Évaluation |
|---|---|
| Confidentialité | Élevée |
| Effacement RGPD | Efficace (crypto-shred) |
| Isolation | Cryptographique (clé/client) |

**Pros :** le client choisit sa politique (accord de traitement) ; clé par client → isolation + crypto-shred instantané ; le corps ne fuit jamais dans logs/traces (invariant testable) ; `content:read` est la frontière d'accès, auditée.
**Cons :** gestion de clés (KMS, rotation, enveloppe) ; le chiffrement au repos ne protège que du vol de base/backup (défense en profondeur, pas frontière d'accès — celle-ci est `content:read`).

### Option B : toujours stocker en clair
**Pros :** simple, requêtable.
**Cons :** exposition PII maximale ; effacement RGPD = suppression massive coûteuse ; inacceptable pour OTP/bancaire.

### Option C : ne jamais stocker le corps
**Pros :** exposition nulle.
**Cons :** prive le tableau de bord de toute consultation/investigation légitime ; certains clients l'exigent contractuellement. Trop rigide comme politique unique (reste disponible via `off`).

## Trade-off Analysis

Le tout-en-clair (B) est un risque PII inacceptable ; le jamais-stocker (C) est trop rigide comme défaut unique. Une politique **configurable** couvre les deux extrêmes (`off` et `stored_*`). La **clé par client** apporte l'isolation cryptographique et surtout le **crypto-shred** : détruire une clé efface tout le contenu d'un client d'un geste, ce qui rend l'effacement RGPD réalisable sans réécriture massive. L'invariant « jamais dans les logs » est orthogonal à la politique de stockage et non négociable.

## Consequences

- **Plus facile :** conformité RGPD (crypto-shred) ; consultation gardée et auditée ; isolation par client.
- **Plus difficile :** gestion de clés (KMS, rotation `active`/`retired`/`destroyed`) ; effacement par MSISDN (clé partagée entre destinataires → suppression ligne à ligne, pas de crypto-shred).
- **Invariant :** anti-spam/segmentation/opt-out lisent le clair **en mémoire avant** stockage ; « traiter » et « ne pas stocker » ne sont pas en conflit.

## Action Items

1. [ ] `internal/…/content` : enveloppe KMS, clé par client, chiffrement à l'écriture CDR uniquement.
2. [ ] Test d'invariant (a) : le corps ne fuit dans aucune sérialisation, sous chaque valeur de `content_storage`.
3. [ ] Effacement RGPD : crypto-shred (client) et suppression ligne à ligne (MSISDN) + attestation.
