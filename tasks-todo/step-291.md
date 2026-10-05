# step-291 — Le trafic d'un expéditeur est contrôlé contre sa catégorie déclarée ; les signalements se comptent

> **Jalon :** ADR-0020 §5 · **Statut :** À FAIRE
> **Dépend de :** step-288 · **Bloque :** le compteur de signalements de l'écran sender IDs du tableau de bord
> (go-gateway-bo step-067)
> Décision humaine du 05/10/2026 (métrique + compteur Redis) ; unité faute de multiple de dix libre.

## Pourquoi
ADR-0020 §5 : une déclaration qui ne se vérifie pas n'engage personne. Aujourd'hui, un `flag` anti-spam ne
laisse **aucune trace** au-delà d'un attribut de span (`internal/pipeline/pipeline.go:203`). Il n'y a ni
compteur, ni métrique, ni stockage, et le tableau de bord n'a rien à afficher.

## Design arrêté
- **Règle `category_mismatch`** : un nouveau `rule_type` (le `CHECK` est étendu par migration, en même temps
  que le schéma). Les portées sont `global`, `customer` et `smpp_account`. Elle s'évalue en mémoire, comme
  les règles de contenu, avant la sortie anticipée de `Evaluate`. La catégorie vient de l'étape sender ID
  (step-288 la stocke ; c'est step-291 qui la fait rendre par `Authorize` et la passe à l'anti-spam). Les
  paramètres, validés par `ValidateRuleConfig` :
  - `otp` : un code de `min_digits` à `max_digits` chiffres (4 et 8 par défaut), aucune URL, au plus
    `max_length` caractères ;
  - `transactional` : aucun des `promo_markers`, une liste configurable ;
  - `marketing` : rien à vérifier.
- **Action** : `flag` par défaut, `block` configurable. Au contrat, `action` sort de `required` pour
  `category_mismatch` et prend `default: flag`. Les autres types restent inchangés, ce qui se tranche au
  design du contrat sans en faire une rupture.
- **Le verdict porte l'identifiant de la règle.** Seuls l'identifiant de règle et le verdict sortent du
  moteur, dans un log, une métrique ou un span (invariant a).
- **Métrique** `antispam_verdicts_total{rule_type, action}`, pour `flag` et `block`, sans label
  d'expéditeur ni de client (garde des labels). Alertmanager s'en sert, et l'alerte persiste côté tableau
  de bord dans `notifications` : c'est là qu'une alerte ratée par un opérateur reste non lue.
- **Compteur par sender ID** : chaque `flag`, quelle que soit la règle, fait un `INCR` sur une tranche
  horaire Redis, clé `(customer_id, address)` + heure, TTL 25 h. L'écriture est en fail-open : une panne
  Redis ne retient pas le message.
- **Contrat Admin, bump MINEUR** : `SenderId.recent_flags_24h` est la somme des 24 dernières tranches,
  servie par `list-sender-ids` en **un seul `MGET`** pour toute la liste. La fenêtre de 24 h glissantes est
  déclarée dans le nom et la description. `null` si Redis ne répond pas : le compte est inconnu, pas nul.
- **Rechargement à chaud des règles** : le moteur anti-spam est chargé une seule fois au démarrage du
  routeur (`cmd/router-svc/wiring.go:375-379`). Il rejoint le watcher de snapshot du routeur, sans quoi
  une règle `category_mismatch` créée au tableau de bord ne s'appliquerait qu'au prochain redémarrage.

## À vérifier avant le contrat
Si la spec v2.1 du tableau de bord (go-gateway-bo) nomme déjà une métrique anti-spam dans `alert_rules`,
la métrique en reprend le nom.

## Tests rouges attendus
- Un OTP déclaré qui porte une URL est signalé. Le même corps envoyé sous un expéditeur `marketing` ne
  l'est pas.
- Un transactionnel qui porte un marqueur promotionnel est signalé. Avec `block`, il est rejeté
  (`ErrContentBlocked`).
- **Invariant a** : les logs capturés du moteur, les attributs de span et les labels de métrique ne
  contiennent aucun extrait du corps ni aucun marqueur trouvé. `AssertNoBody` est ajouté à
  `TestPipelineSpamFlagDoesNotBlock`, qui ne l'appelle pas aujourd'hui.
- `recent_flags_24h` compte un signalement de l'heure courante et oublie une tranche de plus de 24 h.
- Une règle créée par l'API Admin s'applique sans redémarrage du routeur.

## Dettes ouvertes par ce design
- `debts/file-de-revue-antispam-inexistante.md` : le compteur n'a pas de file de revue vers laquelle
  pointer.
- `debts/signalements-n-alimentent-pas-la-reputation.md` : ADR-0020 §5 dit que le flag alimente la
  réputation, et personne n'écrit `antispam:rep:`.
