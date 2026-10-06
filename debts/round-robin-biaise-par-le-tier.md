# Round-robin biaisé par le tier

> **Statut :** OUVERTE · **Nature :** technique
> **Née de :** step-292 (PR2) · **Portée par :** —

**Ce qu'on a fait à la place.** Le round-robin tire son index d'un compteur par route (`rrNext`), modulo
la liste des cibles **filtrée par le tier** du message (`internal/routing/snapshot.go:349`). Les rangs se
partagent ce compteur. Sur une route qui mêle un connecteur réservé et un connecteur partagé, un trafic
alterné marketing puis OTP peut placer chaque OTP sur un index fixe : tous les OTP partent alors sur le même
connecteur.

**Pourquoi.** Un compteur par couple (route, rang) triple l'état du round-robin pour un cas qu'aucune mesure
ne montre. Ni la correction ni le tier ne sont en jeu : aucun connecteur réservé n'est jamais rendu à un
rang inférieur.

**Ce qu'il en coûte.** Une charge inégale entre les connecteurs d'une route round-robin mixte, pour le rang
minoritaire.

**À quoi on reconnaîtra qu'il faut la payer.** La charge par connecteur d'une route round-robin à cibles
réservées s'écarte nettement de l'égalité, ou un opérateur configure une telle route en production.
