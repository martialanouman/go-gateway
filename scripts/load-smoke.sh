#!/usr/bin/env bash
#
# Fumigation du harnais de charge : prouve que les seuils k6 sont câblés (step-200 D3).
#
# Un run de charge contre un stub local passe TRIVIALEMENT — le stub répond en sub-milliseconde,
# n'importe quel budget de latence est respecté. Un tel run vert ne prouve donc rien. La preuve est
# le couple : le même script doit ÉCHOUER contre un stub délibérément ralenti au-dessus du budget.
# Si les deux runs passent, les seuils ne sont pas branchés et le harnais est décoratif.
#
# Ce script ne mesure pas les NFR — il ne dit rien de la tenue à 8 000 req/s, qui se vérifie sur
# matériel réel (step-201). Il vérifie l'instrument, pas le système.
#
# Usage : scripts/load-smoke.sh

set -euo pipefail

cd "$(dirname "$0")/.."

K6="${K6:-k6}"
SCRIPT="test/load/k6/messages.js"
ADDR="${LOAD_STUB_ADDR:-127.0.0.1:8099}"
# Doit dépasser le budget encodé dans le script (p99 < 250 ms), avec de la marge.
SLOW_DELAY="${LOAD_SLOW_DELAY:-300ms}"

# D7 : jamais de skip. Un test qui se dérobe quand son outil manque est vert sans avoir rien vérifié.
if ! command -v "$K6" >/dev/null 2>&1; then
  cat >&2 <<EOF
load-smoke: k6 est introuvable.

k6 est un binaire hors go.mod (plan §1.3, requis à M12) : il s'installe à part.
  macOS   : brew install k6
  Linux   : voir https://grafana.com/docs/k6/latest/set-up/install-k6/

Ce script échoue au lieu de se skipper : un harnais de charge qu'on croit vert
parce qu'il ne s'est pas exécuté est pire que pas de harnais du tout.
EOF
  exit 2
fi

HOST="${ADDR%:*}"
PORT="${ADDR##*:}"

# Le stub est COMPILÉ puis lancé directement, jamais via `go run` : `go run` exécute le binaire dans
# un processus enfant, si bien que tuer son PID laisse le serveur vivant et le port occupé. Le run
# suivant taperait alors l'ancien stub — celui qui n'est pas ralenti — et passerait le seuil sans
# que rien ne le signale.
STUB_BIN="$(mktemp -t load-stub.XXXXXX)"
go build -o "$STUB_BIN" ./cmd/load-stub

STUB_PID=""
cleanup() {
  if [[ -n "$STUB_PID" ]]; then
    kill "$STUB_PID" 2>/dev/null || true
    wait "$STUB_PID" 2>/dev/null || true
    STUB_PID=""
  fi
  # Le port doit être réellement rendu avant le run suivant, sinon on mesure le stub précédent.
  for _ in {1..50}; do
    nc -z "$HOST" "$PORT" 2>/dev/null || return 0
    sleep 0.2
  done
  echo "load-smoke: $ADDR est toujours occupé après l'arrêt du stub" >&2
  return 1
}
trap 'cleanup || true; rm -f "$STUB_BIN"' EXIT

# start_stub <délai> — démarre le stub et attend qu'il accepte réellement une connexion.
start_stub() {
  if nc -z "$HOST" "$PORT" 2>/dev/null; then
    echo "load-smoke: $ADDR est déjà occupé — un run mesurerait le mauvais serveur" >&2
    return 1
  fi
  "$STUB_BIN" -addr "$ADDR" -delay "$1" &
  STUB_PID=$!
  for _ in {1..50}; do
    if nc -z "$HOST" "$PORT" 2>/dev/null; then return 0; fi
    sleep 0.2
  done
  echo "load-smoke: le stub n'écoute pas sur $ADDR après 10 s" >&2
  return 1
}

# Les deux runs capturent le code de sortie de k6 lui-même. Surtout pas de pipe vers tail ici :
# le code de sortie serait celui de tail, et un seuil violé passerait pour un succès.
echo "==> run POSITIF — stub sans délai, le budget doit être tenu"
start_stub 0
set +e
BASE_URL="http://$ADDR" "$K6" run --quiet "$SCRIPT"
positive=$?
set -e
cleanup

if [[ $positive -ne 0 ]]; then
  echo "load-smoke: ÉCHEC — le run positif aurait dû passer (exit $positive)" >&2
  exit 1
fi

echo
echo "==> run NÉGATIF — stub ralenti à $SLOW_DELAY, le seuil doit tomber"
start_stub "$SLOW_DELAY"
set +e
BASE_URL="http://$ADDR" "$K6" run --quiet "$SCRIPT"
negative=$?
set -e
cleanup

if [[ $negative -eq 0 ]]; then
  echo "load-smoke: ÉCHEC — le run négatif est passé alors que le stub dépasse le budget." >&2
  echo "            Les seuils ne sont pas câblés : le harnais ne mesure rien." >&2
  exit 1
fi

echo
echo "load-smoke: OK — le harnais passe à vide (exit 0) et tombe sous contrainte (exit $negative)."
