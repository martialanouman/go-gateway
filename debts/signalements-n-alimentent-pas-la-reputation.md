# Les signalements anti-spam n'alimentent pas la réputation : personne n'écrit `antispam:rep:`

> **Statut :** OUVERTE · **Nature :** technique
> **Née de :** l'exploration de step-291 (05/10/2026) · **Portée par :** —

**Ce qu'on a fait à la place.** La règle `reputation` lit un score par expéditeur dans Redis
(`internal/pipeline/antispam/redis.go:103`, clé `antispam:rep:<source>`). Aucun code n'écrit cette clé, et
step-291 ne l'écrit pas non plus : il compte les signalements par sender ID, sans toucher au score.

**Pourquoi.** ADR-0020 §5 dit qu'un `flag` « alimente la réputation (§6.5) », mais la formule du score
(poids d'un signalement, décroissance, plancher) n'est écrite nulle part. step-291 avait besoin d'un
compteur, pas d'une politique de réputation.

**Ce qu'il en coûte si on ne la paie jamais.** Une règle `reputation` configurée ne bloque jamais rien :
elle lit toujours une clé absente. L'opérateur croit qu'une protection est active alors qu'elle ne l'est
pas.

**À quoi on reconnaîtra qu'il faut la payer.** La première règle `reputation` créée en environnement réel,
ou une revue de §6.5.
