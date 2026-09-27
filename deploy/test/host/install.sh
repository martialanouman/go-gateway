#!/usr/bin/env bash
# Préparation d'un VPS Ubuntu 24.04 neuf, en root, une fois : k3s, pare-feu, compte de CD.
# usage : install.sh "<clé publique ssh de la CD>"
set -euo pipefail
pubkey=${1:?"usage: $0 <clé publique ssh de la CD>"}
here=$(cd "$(dirname "$0")" && pwd)

apt-get update -q && apt-get install -yq ufw
ufw default deny incoming
ufw default allow outgoing
ufw allow 22/tcp
# Réseaux des pods et des Services de k3s : sans eux, ufw coupe le trafic entre pods.
ufw allow from 10.42.0.0/16
ufw allow from 10.43.0.0/16
ufw --force enable
sed -i 's/^#\?PasswordAuthentication .*/PasswordAuthentication no/' /etc/ssh/sshd_config
systemctl reload ssh

curl -sfL https://get.k3s.io | sh -s - --write-kubeconfig-mode 600
until kubectl get nodes >/dev/null 2>&1; do sleep 2; done
systemctl show k3s -p LimitNOFILE

kubectl create namespace gateway --dry-run=client -o yaml | kubectl apply -f -
kubectl apply -f - <<'EOF'
apiVersion: v1
kind: ServiceAccount
metadata: {name: deploy, namespace: gateway}
---
apiVersion: rbac.authorization.k8s.io/v1
kind: Role
metadata: {name: deploy, namespace: gateway}
rules:
  - apiGroups: ["*"]
    resources: ["*"]
    verbs: ["*"]
---
apiVersion: rbac.authorization.k8s.io/v1
kind: RoleBinding
metadata: {name: deploy, namespace: gateway}
roleRef: {apiGroup: rbac.authorization.k8s.io, kind: Role, name: deploy}
subjects: [{kind: ServiceAccount, name: deploy, namespace: gateway}]
---
apiVersion: v1
kind: Secret
metadata:
  name: deploy-token
  namespace: gateway
  annotations: {kubernetes.io/service-account.name: deploy}
type: kubernetes.io/service-account-token
EOF
until token=$(kubectl -n gateway get secret deploy-token -o jsonpath='{.data.token}' | base64 -d) && [[ -n $token ]]; do sleep 1; done

id deploy >/dev/null 2>&1 || useradd -m -s /bin/bash deploy
install -d -m 700 -o deploy -g deploy /home/deploy/.kube /home/deploy/.ssh
KUBECONFIG=/home/deploy/.kube/config kubectl config set-cluster k3s \
  --server=https://127.0.0.1:6443 \
  --certificate-authority=/var/lib/rancher/k3s/server/tls/server-ca.crt --embed-certs
KUBECONFIG=/home/deploy/.kube/config kubectl config set-credentials deploy --token="$token"
KUBECONFIG=/home/deploy/.kube/config kubectl config set-context deploy --cluster=k3s --user=deploy --namespace=gateway
KUBECONFIG=/home/deploy/.kube/config kubectl config use-context deploy
chown deploy:deploy /home/deploy/.kube/config && chmod 600 /home/deploy/.kube/config

install -m 755 "$here/gateway-deploy" /usr/local/bin/gateway-deploy
printf 'command="/usr/local/bin/gateway-deploy",no-port-forwarding,no-X11-forwarding,no-agent-forwarding,no-pty %s\n' "$pubkey" \
  >/home/deploy/.ssh/authorized_keys
chown deploy:deploy /home/deploy/.ssh/authorized_keys && chmod 600 /home/deploy/.ssh/authorized_keys
echo "install.sh: prêt. Empreinte de l'hôte pour TEST_SSH_KNOWN_HOSTS :"
ssh-keyscan -t ed25519 127.0.0.1 2>/dev/null | sed "s/^127.0.0.1/$(curl -s4 ifconfig.me)/"
