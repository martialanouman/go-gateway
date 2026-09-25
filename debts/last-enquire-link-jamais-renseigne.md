# `last_enquire_link` n'est jamais renseigné

> **Statut :** OUVERTE · **Nature :** produit
> **Née de :** step-360 (design arrêté, point 1) · **Portée par :** —

**Ce qu'on a fait à la place.** Le champ `last_enquire_link` de `Session` est toujours absent (null)
dans `list-sessions` et `list-account-sessions` (`internal/adminapi/sessions.go:46`) : le
registre ne le stocke pas.

**Pourquoi.** Le suivre coûterait une écriture Redis par `enquire_link` reçu, sur chaque bind, pour une
information de diagnostic ; le rafraîchissement du jeton (`refreshLoop`) ne passe qu'à mi-TTL et ne
dit rien du trafic du pair.

**Ce qu'il en coûte.** Un opérateur ne distingue pas, depuis la liste, un bind silencieux d'un bind
actif ; il doit attendre que `IdleTimeout` le coupe.

**À quoi on reconnaîtra qu'il faut la payer.** Des tickets « bind fantôme » où la question est « depuis
quand le client ne parle-t-il plus ? ».
