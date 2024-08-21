#!/bin/bash -e
# find os
case `uname -s` in
Darwin) OS="apple-darwin";;
Linux) OS="unknown-linux-gnu";;
*) fail "unknown os: $(uname -s)";;
esac
#find ARCH
if uname -m | grep -E '(arm|arch)64' > /dev/null; then
  ARCH="aarch64"
elif uname -m | grep 64 > /dev/null; then
  ARCH="x86_64"
else
  fail "unknown arch: $(uname -m)"
fi


# Potential common directories to place binaries
directories=("/usr/local/bin" "/usr/bin" "/bin" "$HOME/bin")

# Loop through the directories and pick the first one that exists and is writable
for dir in "${directories[@]}"; do
  if [[ -d "$dir" && -w "$dir" ]]; then
    echo "Found a writable binary directory: $dir"
    BIN_DIR=$dir
    break
  fi
done

# Check if a suitable directory was found
if [ -z "$BIN_DIR" ]; then
  echo "No suitable binary directory found. You may need to create one with appropriate permissions."
  exit 1
fi

VERSION=$1
if [ -z "$VERSION" ]; then
  FNAME=$(aws s3 ls "s3://repo.propellerheads/tycho-client/tycho-client-${ARCH}" | grep -v 'pre' | sort -k1,2 | tail -n1 | awk '{print $4}')
  echo "installing latest version: ${FNAME}"
else
  FNAME="tycho-client-${ARCH}-${OS}-${VERSION}.tar.gz"
fi

RELEASE="s3://repo.propellerheads/tycho-client/${FNAME}"
aws s3 cp $RELEASE - | tar -xz
xattr -d com.apple.quarantine tycho-client 2>/dev/null || true
chmod +x tycho-client
mv tycho-client $BIN_DIR
echo "done"