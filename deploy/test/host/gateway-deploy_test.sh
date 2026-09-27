#!/usr/bin/env bash
set -euo pipefail
here=$(cd "$(dirname "$0")" && pwd)
tmp=$(mktemp -d); trap 'rm -rf "$tmp"' EXIT
mkdir "$tmp/bin"
cat >"$tmp/bin/kubectl" <<'EOF'
#!/usr/bin/env bash
echo "$*" >>"$KUBECTL_LOG"
case "$*" in
  *"get statefulset,deployment"*) echo statefulset.apps/postgres ;;
  *"get deployment"*) echo deployment.apps/router-svc ;;
  *"wait"*) [[ -n ${FAIL_WAIT:-} ]] && exit 1 ;;
esac
exit 0
EOF
chmod +x "$tmp/bin/kubectl"
fail() { echo "gateway-deploy_test: $*" >&2; exit 1; }

export KUBECTL_LOG="$tmp/log"
echo 'kind: ConfigMap' | PATH="$tmp/bin:$PATH" "$here/gateway-deploy" >/dev/null
order=$(grep -nE 'apply .*phase=deps$|delete job -l gateway.test/phase=deps-job|delete job -l gateway.test/phase=job|apply .*!gateway.test/phase|rollout status deployment.apps/router-svc' "$KUBECTL_LOG" | cut -d: -f1 | tr '\n' ' ')
order="${order% }"
# wc -w padde sa sortie sur macOS (BSD) : comparaison arithmétique, pas lexicale.
(( $(wc -w <<<"$order") == 5 )) || fail "étapes manquantes : $(cat "$KUBECTL_LOG")"
sorted=$(tr ' ' '\n' <<<"$order" | sort -n | tr '\n' ' ')
sorted="${sorted% }"
[[ $order == "$sorted" ]] || fail "ordre violé : $(cat "$KUBECTL_LOG")"

: >"$KUBECTL_LOG"
if echo 'kind: ConfigMap' | FAIL_WAIT=1 PATH="$tmp/bin:$PATH" "$here/gateway-deploy" >/dev/null 2>&1; then
  fail "un Job en échec n'arrête pas le déploiement"
fi
! grep -q '!gateway.test/phase' "$KUBECTL_LOG" || fail "l'application est appliquée malgré un Job en échec"
echo "gateway-deploy_test: ok"
