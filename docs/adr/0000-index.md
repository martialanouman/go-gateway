# Architecture Decision Records — Passerelle SMS

Les décisions structurantes de la passerelle, une par fichier, au format ADR standard. Elles sont **déjà tranchées** dans la spécification technique (§7 Évaluation) ; ces ADR les figent pour éviter de rejouer les débats en cours d'implémentation. Statut par défaut : `Accepted`.

| ADR | Décision | Statut |
|---|---|---|
| [0001](0001-kafka-ingestion-durable.md) | Kafka comme couche d'ingestion durable | Accepted |
| [0002](0002-soldes-mt-mo-separes.md) | Soldes MT et MO séparés ; MO = compteur postpayé | Accepted |
| [0003](0003-moteur-script-routage-embarque.md) | Moteur de script de routage embarqué (goja/Lua) vs FaaS | Accepted |
| [0004](0004-routage-3-niveaux-numero-exact.md) | Routage à 3 niveaux avec court-circuit numéro exact | Accepted |
| [0005](0005-disjoncteur-etat-hybride.md) | Disjoncteur par connecteur à état hybride | Accepted |
| [0006](0006-client-compte-smpp-distincts.md) | Client et compte SMPP distincts ; cardinalité des identifiants | Accepted |
| [0007](0007-opt-out-scope-canal.md) | Opt-out scopé au canal avec union à l'application | Accepted |
| [0008](0008-contenu-chiffre-cle-client.md) | Stockage de contenu chiffré par clé client, jamais loggé | Accepted |
| [0009](0009-annulation-reservee-smpp.md) | Annulation d'un message réservée au canal SMPP (pas de surface REST) | Accepted |
| [0010](0010-config-billing-sur-customers.md) | Configuration de facturation consolidée sur `customers` | Accepted |
| [0011](0011-content-key-svc-dedie.md) | La garde des clés de contenu vit dans un service dédié | Accepted |
| [0012](0012-duplication-submit-sm-bornee.md) | Duplication d'un `submit_sm` assumée et **bornée** (~250 par partition et par crash) | Accepted |
| [0013](0013-annulation-jeton-vainqueur-unique.md) | `cancelled` signifie « jamais parti » ; arbitrage par jeton à vainqueur unique | Accepted |
| [0014](0014-duplication-au-routeur.md) | La duplication a **deux** causes ; la seconde est au routeur, bornée par la même grandeur | Accepted |

## Convention

Un nouvel ADR reçoit le numéro suivant, statut `Proposed`, et passe `Accepted` après revue. Une décision qui en remplace une autre référence l'ancienne (`Supersedes ADR-XXXX`) et passe l'ancienne à `Superseded`. On n'édite jamais le fond d'un ADR accepté : on en écrit un nouveau.
