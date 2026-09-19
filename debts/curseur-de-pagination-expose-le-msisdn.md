# Le curseur de pagination des routes exactes expose le MSISDN en clair

> **Statut :** OUVERTE · **Nature :** produit
> **Née de :** step-260j (`tasks-done/step-260j.md:24`) · **Portée par :** —

step-260j a unifié trois codecs de curseur dans `platform/keyset` et a **exclu celui-ci** de la
migration, en proposant de le traiter « avec step-390 ou seul ». `tasks-todo/step-390.md` ne le
mentionne pas : son sujet est les réglages de compte. Le rattachement n'a donc jamais eu lieu.

**Ce qu'il en coûte.** Non écrite. En pratique : un numéro d'abonné voyage dans un jeton d'API que le
client paginera, journalisera et recopiera dans ses propres traces — de la PII hors du périmètre
qu'on croit contrôler.

**À quoi on reconnaîtra qu'il faut la payer.** Au premier audit qui demande où les MSISDN circulent,
ou à la migration des curseurs restants.

Source : `internal/storage/postgres/exact_routes.go:38`
