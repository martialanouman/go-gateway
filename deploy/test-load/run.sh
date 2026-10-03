#!/usr/bin/env bash
# Campagne step-280 sur le VPS de test (README §11). Accès root SSH de l'exploitant.
#
#   run.sh HOST apply                      leviers de campagne (2 réplicas figés)
#   run.sh HOST seed VERSION PORTED_SHARE [CUSTOMERS]
#                                          clients de charge (24 par défaut) ; clés dans le Secret k6-load
#   run.sh HOST ceiling VERSION            plafond du simulateur, dans le cluster
#   run.sh HOST k6 PROFILE IDEMPOTENCY DURATION
#   run.sh HOST observe MINUTES            toutes les 10 s : sessions Postgres en attente d'un verrou sur
#                                          balances, et submits_total cumulé du pool (step-284)
set -euo pipefail

host=$1 action=$2
here=$(cd "$(dirname "$0")" && pwd)
root=$here/../..
# ssh recolle ses arguments en une chaîne que le shell distant réinterprète : sans %q, le jsonpath
# de run_job casse et la boucle d'attente ne rend jamais la main.
# shellcheck disable=SC2029 # expansion côté client voulue : %q protège chaque argument.
# Une seule connexion multiplexée : un sshd public sous brute-force refuse les poignées de main au-delà
# de MaxStartups, et run_job en ouvre une toutes les 10 s.
kube() {
  ssh -o ControlMaster=auto -o ControlPath="$HOME/.ssh/cm-%C" -o ControlPersist=10m \
    "$host" "kubectl -n gateway $(printf '%q ' "$@")"
}

# Un Job est immuable : on le remplace, et on attend qu'il se termine en sondant ses conditions
# (`kubectl wait --for=condition=complete` ne rend jamais sur un Job Failed).
run_job() {
  local name=$1 manifest=$2
  kube delete job "$name" --ignore-not-found --wait
  kube apply -f - <<<"$manifest"
  until kube get job "$name" -o jsonpath='{.status.conditions[?(@.status=="True")].type}' | grep -qE 'Complete|Failed'; do
    sleep 10
  done
  kube logs "job/$name"
  kube get job "$name" -o jsonpath='{.status.conditions[?(@.status=="True")].type}' | grep -q Complete
}

case $action in
  # Jamais un `kubectl scale` à la main : les valeurs vivent ici. L'HPA ramène les réplicas dans [min, max]
  # avant de lire ses métriques. min = max, parce qu'avec les requests de
  # 50m du VPS un HPA CPU sature dès la première seconde et ne mesure rien. Le prochain déploiement de
  # main les écrase — pas de merge pendant une campagne.
  apply)
    for d in rest-api-svc router-svc connector-pool-svc; do
      kube patch hpa "$d" --type=merge -p '{"spec":{"minReplicas":2,"maxReplicas":2}}'
    done
    ;;
  seed)
    manifest=$(sed -e "s/@VERSION@/$3/" -e "s/@PORTED_SHARE@/$4/" -e "s/@CUSTOMERS@/${5:-24}/" "$here/seed-load.yaml")
    # La clé n'est imprimée qu'une fois : on la range dans un Secret, puis on efface le Job qui la porte
    # dans ses logs.
    trap 'kube delete job seed-load --ignore-not-found' EXIT
    out=$(run_job seed-load "$manifest") || { echo "$out" >&2; exit 1; }
    key=$(sed -n 's/^API_KEYS=//p' <<<"$out")
    [[ -n $key ]] || { echo "seed-load n'a rendu aucune clé" >&2; exit 1; }
    # Rendu en local, par un fichier : les clés ne passent dans aucune ligne de commande.
    kubectl create secret generic k6-load --from-env-file=<(printf 'API_KEYS=%s\n' "$key") --dry-run=client -o yaml |
      kube apply -f -
    # billing-svc ne relit la config client que toutes les 30 s : avant, le client est prépayé strict et
    # le début d'un run serait refusé en aval, sans bruit, derrière des 202.
    sleep 35
    ;;
  # Les binds du pool restent ouverts sur le simulateur, et smsc-ceiling disqualifie tout palier où le
  # pair tient plus de binds qu'il n'en a demandé. À 0 réplica, l'HPA se suspend de lui-même.
  ceiling)
    trap 'kube scale deployment connector-pool-svc --replicas=2' EXIT
    kube scale deployment connector-pool-svc --replicas=0
    kube wait --for=delete pod -l app=connector-pool-svc --timeout=200s || true
    run_job smsc-ceiling "$(sed "s/@VERSION@/$3/" "$here/ceiling.yaml")"
    ;;
  k6)
    kubectl create configmap k6-script --from-file="$root/test/load/k6/messages.js" --dry-run=client -o yaml | kube apply -f -
    run_job k6-load "$(sed -e "s/@PROFILE@/$3/" -e "s/@IDEMPOTENCY@/$4/" -e "s/@DURATION@/$5/" "$here/k6.yaml")"
    ;;
  # Le relevé de step-284 : la ligne de solde d'un client ne doit plus faire attendre personne. Le débit de
  # traversée est la pente de submits_total entre deux lignes, lu par le proxy de l'API (pas de curl dans
  # les images distroless).
  observe)
    end=$((SECONDS + $3 * 60))
    echo "time waiting_on_balances submits_total"
    while ((SECONDS < end)); do
      # shellcheck disable=SC2016 # $POSTGRES_USER/$POSTGRES_DB s'étendent dans le conteneur, pas ici.
      waiting=$(kube exec postgres-0 -- sh -c 'psql -U "$POSTGRES_USER" -d "$POSTGRES_DB" -tAc "SELECT count(*) FROM pg_stat_activity WHERE wait_event_type = '\''Lock'\'' AND query ILIKE '\''%balances%'\''"')
      submits=0
      for pod in $(kube get pods -l app=connector-pool-svc -o jsonpath='{.items[*].metadata.name}'); do
        n=$(kube get --raw "/api/v1/namespaces/gateway/pods/$pod:9090/proxy/metrics" |
          awk '/^submits_total/ {s += $NF} END {printf "%d", s}')
        submits=$((submits + n))
      done
      echo "$(date -u +%H:%M:%S) $waiting $submits"
      sleep 10
    done
    ;;
  *)
    echo "usage: run.sh HOST apply|seed|ceiling|k6|observe …" >&2
    exit 2
    ;;
esac
