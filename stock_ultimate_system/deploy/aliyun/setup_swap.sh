#!/usr/bin/env bash
set -euo pipefail

SWAPFILE=${SWAPFILE:-/swapfile}
SIZE_GB=${SIZE_GB:-8}

if swapon --show | grep -q "^${SWAPFILE} "; then
  echo "swap already active: ${SWAPFILE}"
  exit 0
fi

if [ ! -f "${SWAPFILE}" ]; then
  fallocate -l "${SIZE_GB}G" "${SWAPFILE}" || dd if=/dev/zero of="${SWAPFILE}" bs=1G count="${SIZE_GB}" status=progress
  chmod 600 "${SWAPFILE}"
  mkswap "${SWAPFILE}"
fi

swapon "${SWAPFILE}"

if ! grep -q "^${SWAPFILE} " /etc/fstab; then
  echo "${SWAPFILE} none swap sw 0 0" >> /etc/fstab
fi

mkdir -p /etc/sysctl.d
cat >/etc/sysctl.d/99-stock-ultimate-swap.conf <<'EOF'
vm.swappiness=20
vm.vfs_cache_pressure=50
EOF
sysctl --system >/dev/null

echo "swap enabled:"
swapon --show
