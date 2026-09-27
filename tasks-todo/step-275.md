# step-275 — Environnement de test : seed du plan de contrôle et preuve bout-en-bout

> **Jalon :** M12 · **Statut :** À FAIRE
> **Dépend de :** l'environnement de test k3s (docs/superpowers/specs/2026-09-27-environnement-de-test-k3s-design.md) · **Bloque :** —

## Pourquoi cette fiche existe

L'environnement de test déploie et prouve que chaque pod est prêt ; il ne prouve pas qu'un SMS
traverse. Deux manques, reportés hors du premier livrable :

1. **Aucun seed.** Sans client, compte, identifiant de bind, sender ID actif, connecteur et route
   statique, rien ne s'envoie. Le connector-pool ne traite que les enregistrements dont `ConnectorID`
   égale son `CONNECTOR_ID` (`internal/connectorpool/submit.go:176`), qui doit donc être l'id d'une
   ligne `smsc_connectors` visée par une route. Le modèle du seed : `internal/e2e/e2e_test.go:221`
   (`seedControlPlane`) ; par l'API Admin, pour que `config:changed` invalide les caches.
2. **Aucune preuve bout-en-bout.** Un Job `smoke` qui binde en TLS, soumet un `submit_sm` avec
   `registered_delivery` et attend son DLR — à condition que smsc-simulator v0.7.0 émette des DLR avec
   la configuration `healthy` (à vérifier dans son dépôt ; `docs/specification-technique-simulateur-smsc.md`).

## Définition de terminé
- [ ] Seed idempotent par l'API Admin, rejouable après une remise à zéro du namespace.
- [ ] `CONNECTOR_ID` de l'overlay égal à l'id du connecteur seedé.
- [ ] Job `smoke` lancé par `gateway-deploy` en dernière phase ; le workflow échoue s'il échoue.
