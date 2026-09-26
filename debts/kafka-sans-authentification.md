# Kafka est le seul magasin joint sans aucune authentification

> **Statut :** OUVERTE · **Nature :** technique
> **Née de :** step-305 · **Portée par :** —

**Ce qu'on a fait à la place.** step-305 chiffre le lien vers Kafka (`KAFKA_TLS_ENABLED`), sans rien
dire de qui parle. Postgres et ClickHouse ont un utilisateur et un mot de passe, Redis porte le sien
dans l'URL. Kafka n'a ni SASL ni certificat client : `internal/storage/kafka/options.go:29` ne pose que
le délai et le TLS, et aucun `sasl` n'existe dans `internal/` ni dans `cmd/`.

**Pourquoi.** Hors du périmètre de step-305, qui portait sur le chiffrement du transport. L'arbitrage
du 2026-09-26 a écarté le mTLS client vers les magasins, faute d'un magasin connu qui l'exige.

**Ce qu'il en coûte si on ne la paie jamais.** Tout ce qui atteint le réseau des brokers peut produire
dans `mt.inbound`, ce qui revient à envoyer des SMS facturés en contournant l'authentification
d'ingestion, ou lire `mt.inbound`, dont le corps voyage en clair dans la valeur de l'enregistrement
(`internal/pipeline/wire.go:30`).

**À quoi on reconnaîtra qu'il faut la payer.** Au premier cluster Kafka qui n'est pas sur un réseau
privé réservé à la passerelle, ou au go-live (step-410), selon ce qui arrive en premier.
