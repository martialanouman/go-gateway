#!/usr/bin/env bash
# Campagne step-280 sur le VPS de test (README §11). Accès root SSH de l'exploitant.
#
#   run.sh HOST apply                      leviers de campagne (2 réplicas figés)
#   run.sh HOST seed VERSION PORTED_SHARE  compte de charge ; la clé API va dans le Secret k6-load
#   run.sh HOST ceiling VERSION            plafond du simulateur, dans le cluster
#   run.sh HOST k6 PROFILE IDEMPOTENCY DURATION
set -euo pipefail

host=$1 action=$2
here=$(cd "$(dirname "$0")" && pwd)
root=$here/../..
# ssh recolle ses arguments en une chaîne que le shell distant réinterprète : sans %q, le jsonpath
# de run_job casse et la boucle d'attente ne rend jamais la main.
# shellcheck disable=SC2029 # expansion côté client voulue : %q protège chaque argument.
kube() { ssh "$host" "kubectl -n gateway $(printf '%q ' "$@")"; }

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
  # Jamais `kubectl scale` à la main : les valeurs vivent ici. min = max, parce qu'avec les requests de
  # 50m du VPS un HPA CPU sature dès la première seconde et ne mesure rien. Le prochain déploiement de
  # main les écrase — pas de merge pendant une campagne.
  apply)
    for d in rest-api-svc router-svc connector-pool-svc; do
      kube patch hpa "$d" --type=merge -p '{"spec":{"minReplicas":2,"maxReplicas":2}}'
      kube patch deployment "$d" --type=merge -p '{"spec":{"replicas":2}}'
    done
    ;;
  seed)
    manifest=$(sed -e "s/@VERSION@/$3/" -e "s/@PORTED_SHARE@/$4/" "$here/seed-load.yaml")
    # La clé n'est imprimée qu'une fois : on la range dans un Secret, puis on efface le Job qui la porte
    # dans ses logs.
    trap 'kube delete job seed-load --ignore-not-found' EXIT
    key=$(run_job seed-load "$manifest" | sed -n 's/^API_KEY=//p')
    [[ -n $key ]] || { echo "seed-load n'a rendu aucune clé" >&2; exit 1; }
    # Rendu en local : la clé ne passe jamais dans une ligne de commande de l'hôte.
    kubectl create secret generic k6-load --from-literal="API_KEY=$key" --dry-run=client -o yaml | kube apply -f -
    # billing-svc ne relit la config client que toutes les 30 s : avant, le client est prépayé strict et
    # le début d'un run partirait en 402.
    sleep 35
    ;;
  ceiling)
    run_job smsc-ceiling "$(sed "s/@VERSION@/$3/" "$here/ceiling.yaml")"
    ;;
  k6)
    kubectl create configmap k6-script --from-file="$root/test/load/k6/messages.js" --dry-run=client -o yaml | kube apply -f -
    run_job k6-load "$(sed -e "s/@PROFILE@/$3/" -e "s/@IDEMPOTENCY@/$4/" -e "s/@DURATION@/$5/" "$here/k6.yaml")"
    ;;
  *)
    echo "usage: run.sh HOST apply|seed|ceiling|k6 …" >&2
    exit 2
    ;;
esac
