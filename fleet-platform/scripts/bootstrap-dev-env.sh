#!/usr/bin/env bash
# =============================================================================
# Fleet Data Platform — Fresh Environment Bootstrap Script
# =============================================================================
# Script này tự động cài đặt toàn bộ dependencies từ A-Z cho máy mới tinh:
#   1. OS packages (Java OpenJDK 11, Python 3.10+, Build tools, Git, Curl)
#   2. Docker Engine & Docker Compose Plugin
#   3. Python Virtual Environment & pip requirements
#   4. Terminal TUI tools (Harlequin, pgcli, VisiData, LazyGit)
#   5. Neovim >= 0.10 + LazyVim (Python/SQL LSP)
#
# Usage:
#   bash scripts/bootstrap-dev-env.sh
# =============================================================================

set -euo pipefail

echo "======================================================================"
echo " 🚀 Fleet Data Platform — Fresh Environment Bootstrap"
echo "======================================================================"
echo ""

# ─────────────────────────────────────────────────────────────────────────────
# 1. System Packages (Ubuntu / Debian)
# ─────────────────────────────────────────────────────────────────────────────
echo "━━━ [1/5] Updating OS & Installing System Packages ━━━"
sudo apt update && sudo apt upgrade -y
sudo apt install -y \
  openjdk-11-jdk \
  python3 \
  python3-pip \
  python3-venv \
  python3-dev \
  build-essential \
  libpq-dev \
  git \
  curl \
  wget \
  unzip \
  ripgrep \
  fd-find \
  htop \
  tree \
  jq \
  net-tools \
  pgcli

# Set default Java Home if not set
export JAVA_HOME="/usr/lib/jvm/java-11-openjdk-amd64"
echo "export JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64" >> ~/.bashrc
echo "  ✓ System tools & Java OpenJDK 11 installed."
echo ""

# ─────────────────────────────────────────────────────────────────────────────
# 2. Docker Engine & Docker Compose
# ─────────────────────────────────────────────────────────────────────────────
echo "━━━ [2/5] Setting up Docker Engine & Docker Compose ━━━"
if ! command -v docker &> /dev/null; then
  sudo apt install -y ca-certificates gnupg lsb-release
  sudo install -m 0755 -d /etc/apt/keyrings
  curl -fsSL https://download.docker.com/linux/ubuntu/gpg | sudo gpg --dearmor -o /etc/apt/keyrings/docker.gpg
  echo "deb [arch=$(dpkg --print-architecture) signed-by=/etc/apt/keyrings/docker.gpg] https://download.docker.com/linux/ubuntu $(lsb_release -cs) stable" | sudo tee /etc/apt/sources.list.d/docker.list > /dev/null
  sudo apt update
  sudo apt install -y docker-ce docker-ce-cli containerd.io docker-compose-plugin
  sudo usermod -aG docker "$USER"
  echo "  ✓ Docker Engine installed. (Note: Run 'newgrp docker' if running without sudo)"
else
  echo "  ✓ Docker already installed."
fi
echo ""

# ─────────────────────────────────────────────────────────────────────────────
# 3. Python Virtual Environment & Dependencies
# ─────────────────────────────────────────────────────────────────────────────
echo "━━━ [3/5] Setting up Python Virtual Environment & Requirements ━━━"
cd "$(dirname "$0")/.."
if [ ! -d ".venv" ]; then
  python3 -m venv .venv
  echo "  ✓ Created virtual environment in .venv/"
fi

# Activate venv
source .venv/bin/activate
pip install --upgrade pip setuptools wheel
pip install -r requirements.txt

echo "  ✓ Python dependencies installed from requirements.txt."
echo ""

# ─────────────────────────────────────────────────────────────────────────────
# 4. Terminal TUI Tools (LazyGit, Harlequin, VisiData)
# ─────────────────────────────────────────────────────────────────────────────
echo "━━━ [4/5] Installing Terminal TUI Tools ━━━"
if ! command -v lazygit &> /dev/null; then
  LAZYGIT_VERSION=$(curl -s "https://api.github.com/repos/jesseduffield/lazygit/releases/latest" | grep -Po '"tag_name": "v\K[^"]*' || echo "0.41.0")
  curl -Lo /tmp/lazygit.tar.gz "https://github.com/jesseduffield/lazygit/releases/latest/download/lazygit_${LAZYGIT_VERSION}_Linux_x86_64.tar.gz"
  tar -xf /tmp/lazygit.tar.gz -C /tmp lazygit
  sudo install /tmp/lazygit /usr/local/bin
  rm -f /tmp/lazygit.tar.gz /tmp/lazygit
  echo "  ✓ LazyGit installed to /usr/local/bin/lazygit"
else
  echo "  ✓ LazyGit already installed."
fi
echo ""

# ─────────────────────────────────────────────────────────────────────────────
# 5. Neovim & LazyVim IDE (Optional / Automated)
# ─────────────────────────────────────────────────────────────────────────────
echo "━━━ [5/5] Checking Neovim & LazyVim ━━━"
if ! command -v nvim &> /dev/null || [[ $(nvim --version | head -n1) < "NVIM v0.10" ]]; then
  echo "  • Installing latest Neovim binary..."
  curl -Lo /tmp/nvim-linux64.tar.gz https://github.com/neovim/neovim/releases/latest/download/nvim-linux64.tar.gz
  sudo rm -rf /opt/nvim
  sudo tar -C /opt -xzf /tmp/nvim-linux64.tar.gz
  sudo ln -sf /opt/nvim-linux64/bin/nvim /usr/local/bin/nvim
  rm -f /tmp/nvim-linux64.tar.gz
  echo "  ✓ Neovim >= 0.10 installed."
fi

if [ ! -d "$HOME/.config/nvim" ]; then
  echo "  • Installing LazyVim starter..."
  git clone https://github.com/LazyVim/starter "$HOME/.config/nvim"
  rm -rf "$HOME/.config/nvim/.git"
  echo "  ✓ LazyVim starter configured in ~/.config/nvim"
fi
echo ""

# ─────────────────────────────────────────────────────────────────────────────
# Self-Test Validation
# ─────────────────────────────────────────────────────────────────────────────
echo "======================================================================"
echo " 🧪 Running Automated Self-Test (Validation)"
echo "======================================================================"
APP_ENV=local pytest tests/test_config.py -v --tb=short

echo ""
echo "======================================================================"
echo " 🎉 Environment Setup Completed Successfully!"
echo "======================================================================"
echo "Để bắt đầu làm việc:"
echo "  1. Kích hoạt môi trường Python: source .venv/bin/activate"
echo "  2. Bật hạ tầng Local Sandbox : make up"
echo "  3. Kiểm tra kết nối           : make smoke-test"
echo "  4. Mở IDE Terminal            : nvim ."
echo "======================================================================"
