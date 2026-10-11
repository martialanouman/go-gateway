# step-287o — La corrélation DLR se garde 24 h par défaut : `DLR_RETENTION` (12h, 24h, 48h)

> **Jalon :** M12 · **Statut :** LIVRÉE
> **Dépend de :** step-044 (corrélation DLR), step-201f (`DLRMap` au banc)
> Demande humaine du 11/10/2026, née du run 14 de step-287 (12,5 M clés `dlrmap:*` dans Redis).

## Pourquoi
connector-pool-svc écrit `dlrmap:{connector:smsc_msg_id}` à chaque envoi accepté ; sans `validity_period`, le
cas courant, la clé vit 72 h. À 8 000 SMS/s, c'est ~2 milliards de clés gardées pour des accusés qui arrivent
presque tous dans l'heure. Un DLR dont la corrélation a expiré n'est ni livré au client ni écrit en CDR
(`dlr_unmapped_total`) : la rétention EST la fenêtre de retour des DLR.

## Design arrêté
- `DLR_RETENTION` (section générale, comme `ROUTER_LANE_WINDOW`), défaut `24h` ; seules `12h`, `24h`, `48h`
  démarrent. Lue par le seul écrivain : `dlrmap.NewRedisMap(rdb, retention)` dans connector-pool-svc ;
  mo-dlr-router-svc ne fait que lire, une clé expirée y produit déjà « plus de DLR ».
- TTL d'une entrée selon le `validity_period` du message :
  - absent, vide ou malformé → la rétention ;
  - relatif (`…R`) → validité + 1 h, borné à [1 h, 72 h] : le client qui demande 48 h de validité sous une
    rétention de 24 h garde ses DLR tardifs (décision humaine), celui qui demande 2 h n'occupe Redis que 3 h ;
  - absolu → 72 h, comme aujourd'hui : non interprété (dérive d'horloge avec le SMSC), et le client a demandé
    une durée, on ne la raccourcit pas.
- 72 h reste le plafond (validité SMS maximale) ; les épingles d'expéditeur `rewrite:{…}` le gardent.
- Conséquence acceptée : un DLR arrivé après la rétention laisse le message `enroute` dans la CDR et sur
  `GET /messages/{id}`.

## Definition of Done
- [x] `ttlForValidity` : la table ci-dessus (unitaire, 5 mutations tuées)
- [x] config : défaut 24h, 12h et 48h acceptés, toute autre durée refusée (2 mutations tuées)
- [x] câblage : la rétention de la config atteint le `DLRMap` du pool (mutation sur le câblage tuée)
