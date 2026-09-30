#!/bin/sh

set -e

echo "Setting up pre-commit hook..."

# core.hooksPath osoittaa suoraan versionhallittuun hooks-hakemistoon, jolloin
# hookin muutokset tulevat voimaan ilman uutta asennusta. Aiemmin hook
# kopioitiin .git/hooks/:iin, eivätkä päivitykset koskaan päätyneet perille.
git config core.hooksPath hooks
chmod +x ./hooks/pre-commit

# Siivoa vanhalla tavalla asennettu kopio, joka jäisi muuten harhauttamaan.
rm -f ./.git/hooks/pre-commit

echo "Pre-commit hook setup completed."
