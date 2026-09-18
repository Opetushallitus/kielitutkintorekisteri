#!/usr/bin/env bash
set -euo pipefail

BASEDIR=$(dirname "$0")
OUTPUT_DIR="$BASEDIR/../uml"
PACKAGE_ROOT="$BASEDIR/../server/src/main/kotlin/fi/oph/kitu"

# Paketit luetaan lähdepuusta, jottei käsin ylläpidetty lista vanhene pakettien
# muuttuessa. Kovakoodattuna listaan jäi aikoinaan poistettuja paketteja, joista
# julkaistiin tyhjiä kaavioita, ja uudet paketit puuttuivat kokonaan.
packages=()
for package_dir in "$PACKAGE_ROOT"/*/; do
  packages+=("$(basename "$package_dir")")
done

# Paketti, jossa ei ole yhtään Spring-beania, tuottaa pelkän rungon ilman rivejä.
# Sellaista ei julkaista, jottei sivulle jää tyhjiä kaavioita.
has_content() {
  grep -qvE '^(@startuml|@enduml|hide members|hide methods|hide <<[A-Za-z]+>> circle)?$' "$1"
}

load_pumls() {
  local env="$1"
  local url="$2"
  local output_dir="$OUTPUT_DIR/$env"
  local package target

  echo "Generate puml files for $env ($url)..."
  echo

  mkdir -p "$output_dir"
  for package in "${packages[@]}"; do
    target="$output_dir/$package.puml"
    if ! curl -fsS "$url/uml/$package" -o "$target"; then
      echo "  VAROITUS: $package — haku epäonnistui, ohitetaan"
      rm -f "$target"
      continue
    fi
    if ! has_content "$target"; then
      echo "  ohitetaan $package (ei beaneja)"
      rm -f "$target"
      continue
    fi
    echo "  $env/$package.puml"
  done
}

generate_images() {
  local env="$1"
  local target_dir="$OUTPUT_DIR/$env"

  echo
  echo "Generate image files for $env..."
  echo

  find "$target_dir" -type f -name "*.puml" | while IFS= read -r file; do
    target_svg="${file//puml/svg}"
    echo "Processing: $file -> $target_svg"
    docker run -e PLANTUML_LIMIT_SIZE=20000 --rm -i dstockhammer/plantuml:latest -tsvg -pipe > "$target_svg" < "$file"
  done
}

generate_markdown() {
  local env="$1"
  local target_dir="$OUTPUT_DIR/$env"
  target_md="$target_dir/index.md"

  echo
  echo "Generate markdown file for $env: $target_md"
  echo

  {
    echo "# Beans-komponentit ympäristössä $env"
    echo
  } > "$target_md"

  find "$target_dir" -type f -name "*.svg" | sort | while IFS= read -r file; do
    filebasename=$(basename "$file")
    name="fi.oph.kitu.${filebasename//.svg/}"
    {
      echo "## $name"
      echo "![$name diagram]($filebasename)"
      echo
    } >> "$target_md"
  done
}

load_and_generate() {
  local env="$1"
  local source_url="$2"

  load_pumls "$env" "$source_url"
  generate_images "$env"
  generate_markdown "$env"
}

for arg in "$@"; do
  if [[ "$arg" == "local" ]]; then
    load_and_generate "local" "http://localhost:8080/kielitutkinnot"
  fi
  if [[ "$arg" == "untuva" ]]; then
    load_and_generate "untuva" "https://virkailija.untuvaopintopolku.fi/kielitutkinnot"
  fi
  if [[ "$arg" == "qa" ]]; then
    load_and_generate "qa" "https://virkailija.testiopintopolku.fi/kielitutkinnot"
  fi
  if [[ "$arg" == "prod" ]]; then
    load_and_generate "prod" "https://virkailija.opintopolku.fi/kielitutkinnot"
  fi
done
