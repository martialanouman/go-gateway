# Un sender ID n'est validé que par sa longueur maximale (20)

> **Statut :** OUVERTE · **Nature :** produit
> **Née de :** demande utilisateur du 03/10/2026 · **Portée par :** —

**Ce qu'on a fait à la place.** `create-sender-id` n'exige qu'une chaîne de 20 caractères au plus. Il n'y a
ni longueur minimale ni jeu de caractères, et la colonne `address` est un `text` sans `CHECK`. Le `from` de
`submit-message` et le `source_addr` SMPP suivent la même règle. La règle attendue est la suivante :
**2 à 11 caractères**, motif `^[a-zA-Z0-9+\-\s]+$`.

**Pourquoi.** Personne ne l'a posée : le contrat a repris la borne SMPP de `source_addr` (20 octets), pas
la contrainte des opérateurs sur un expéditeur alphanumérique (11 caractères en GSM 03.38).

**Ce qu'il en coûte.** Un sender ID d'un caractère, accentué ou de 20 caractères s'enregistre et
s'approuve. Il est ensuite tronqué, réécrit ou rejeté par le SMSC, après l'ACK : une perte silencieuse
facturée au client.

**À quoi on reconnaîtra qu'il faut la payer.** Le premier rejet SMSC sur un `source_addr` invalide, ou
l'onboarding d'un opérateur qui l'exige. Trois points sont à trancher au paiement :

- **Le cas numérique.** Une adresse numérique E.164 compte jusqu'à 16 caractères avec le `+`, et
  `senderid.isNumeric` l'accepte. La borne de 11 la refuserait. La règle vise-t-elle l'alphanumérique
  seul ?
- **Le `\s`.** Il admet tabulation et saut de ligne. Faut-il se limiter à l'espace ?
- **Le contrat.** Durcir `SenderIdCreate` est une rupture : bump **majeur** de `api/package.json`. Les
  lignes déjà en base qui violent la règle doivent être recensées avant d'ajouter un `CHECK`.

Sources : `internal/adminapi/sender_ids.go:42` · `api/openapi-admin.yaml:2137` ·
`db/schema_passerelle_sms.sql:399` · `internal/restapi/messages.go:36` ·
`internal/pipeline/senderid/senderid.go:114`
