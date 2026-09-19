# Un compte suspendu finit par s'entendre répondre « mot de passe invalide »

> **Statut :** OUVERTE (assumée) · **Nature :** produit
> **Née de :** step-260c (`tasks-done/step-260c.md:237`) · **Portée par :** —

step-260c a sorti `ESME_RSYSERR` du compteur anti-brute-force — une panne de notre côté ne doit pas
verrouiller un client. `ESME_RBINDFAIL` (compte suspendu, canal SMPP désactivé, mauvais type de bind)
continue de l'alimenter, et c'est **délibéré** : « marteler un identifiant désactivé *est* un signal
de brute-force ».

**Ce qu'il en coûte.** Non écrite. Par symétrie avec le cas corrigé : au sixième bind, un ESME
légitime d'un compte suspendu lit `ESME_RINVPASWD` et part chercher un problème de secret qui
n'existe pas.

**À quoi on reconnaîtra qu'il faut la payer.** Un ticket support qui commence par « nos identifiants
ne marchent plus » alors que le compte est simplement suspendu.

Source : `internal/smppserver/listener.go:202`
