#!/bin/bash

CLI_NAME="enso"
BINARY_NAME="enso"
INSTALL_PATH="/usr/local/bin/$CLI_NAME"
RELEASE_URL="https://github.com/aether-flux/ensodb/releases/latest/download/$BINARY_NAME"

echo "🚀 Installing $CLI_NAME..."

# Check dependencies
if ! command -v curl >/dev/null 2>&1; then
    echo "❌ 'curl' is required but not installed."
    exit 1
fi

# Download the binary
echo "📦 Downloading latest release from GitHub..."
curl -fsSL "$RELEASE_URL" -o "$BINARY_NAME"

# Make it executable
chmod +x "$BINARY_NAME"

# Move to /usr/local/bin as 'enso'
if command -v sudo >/dev/null 2>&1; then
    sudo mv "$BINARY_NAME" "$INSTALL_PATH"
else
    echo "⚠️ 'sudo' not found. Attempting to install without it..."
    mv "$BINARY_NAME" "$INSTALL_PATH" || {
        echo "❌ Failed to move binary to $INSTALL_PATH. Try running as root."
        exit 1
    }
fi

# Confirm install
echo "✅ $CLI_NAME installed successfully!"

