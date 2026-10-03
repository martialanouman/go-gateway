# Le pool de connecteurs peut re-soumettre un `submit_sm` à chaque tentative de rejeu

> **Statut :** OUVERTE · **Nature :** technique
> **Née de :** step-285 (design arrêté, point 9) · **Portée par :** —

**Ce qu'on a fait à la place.** Quand un `submit_sm` est parti mais que la suite échoue, le consommateur
rejoue l'enregistrement en place, après un backoff de 1 à 30 s, et le re-soumet. Deux cas sont concernés :
le produce `mt.outcome` échoue, ou `submit_sm_resp` expire alors que le SMSC a reçu le message. Avant
step-285, ce même enregistrement était re-soumis une fois par redémarrage du processus, avec un backoff
Kubernetes qui monte à 5 min.

**Pourquoi.** Seul l'enregistrement en échec de chaque shard est concerné : la voie s'arrête dessus, et
les suivants ne sont jamais partis. On parle donc d'au plus un doublon par shard et par tentative, loin
des ~250 « par partition et par crash » de l'ADR-0012. Un échec de produce coûte aussi
`FailClosedProduceTimeout` (30 s) par tentative, ce qui espace déjà les essais.

**Ce qu'il en coûte.** Sur une longue panne du produce, environ deux fois plus de re-soumissions de ce
message qu'avec les redémarrages. Le chiffre de l'ADR-0012 reste vrai, mais ce chemin s'y ajoute sans y
figurer.

**À quoi on reconnaîtra qu'il faut la payer.** La première rafale de doublons attribuée à une panne de
`mt.outcome`. Paiement : une sentinelle `kafka.ErrFatal` que le pool enveloppe sur ce seul chemin, pour
revenir au redémarrage.

Sources : `internal/connectorpool/submit.go` (`processOne`, `settleOutcome`, `healthRetry`) ·
`internal/storage/kafka/consumer.go` (`Consumer.replay`) · `docs/adr/0012-duplication-submit-sm-bornee.md`
