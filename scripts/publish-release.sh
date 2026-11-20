#!/usr/bin/env bash
# Called by the serialized main-branch CI run after both native image jobs pass.
set -euo pipefail

repo="repos/$GITHUB_REPOSITORY"
image="ghcr.io/${GITHUB_REPOSITORY,,}"
tip=$(gh api "$repo/git/ref/heads/main" --jq .object.sha)
if [[ "$tip" != "$GITHUB_SHA" ]]; then
    echo "A newer main commit is queued for verification."
    exit 0
fi

version=$(git tag --points-at "$GITHUB_SHA" --sort=-version:refname |
    sed -nE '/^[0-9]{4}\.[0-9]{2}\.[0-9]+$/p' | sed -n '1p')
state=unreserved
if [[ -n "$version" ]]; then
    # A retry can find a reserved tag, an incomplete draft, or a public release.
    state=$(gh api --paginate "$repo/releases?per_page=100" |
        jq -r --arg tag "$version" '.[] | select(.tag_name == $tag) | if .draft then "draft" else "published" end')
else
    month=$(date -u +%Y.%m)
    micro=$(git tag --list "$month.*" | awk -F. '
        NF == 3 && $3 ~ /^[0-9]+$/ && $3 + 1 > next_micro {next_micro = $3 + 1}
        END {print next_micro + 0}')
    version="$month.$micro"
fi

printf '%s\n' "$GH_TOKEN" | docker login ghcr.io --username "$GITHUB_ACTOR" --password-stdin
if [[ "$state" != published ]]; then
    work=$(mktemp -d)
    for arch in amd64 arm64; do
        dir="$work/candidates/$arch"
        gh run download "$GITHUB_RUN_ID" --repo "$GITHUB_REPOSITORY" \
            --name "svincolo-review-$arch" --dir "$dir"
        (cd "$dir" && sha256sum --check SHA256SUMS)
    done
    digest=$(python3 scripts/prepare-release.py "$work/candidates" "$work")
    cd "$work/assets"
    sha256sum svincolo-linux-* > SHA256SUMS
    cosign sign-blob --yes --bundle SHA256SUMS.sigstore.json SHA256SUMS

    # Reserve the version for this commit before writing a registry tag.
    if [[ "$state" == unreserved ]]; then
        gh api --method POST "$repo/git/refs" -f "ref=refs/tags/$version" -f "sha=$GITHUB_SHA" >/dev/null
    fi
    skopeo copy --all --preserve-digests "oci:$work/image:release" "docker://$image:$version"
    actual=$(skopeo inspect --raw "docker://$image:$version" | sha256sum)
    [[ "sha256:${actual%% *}" == "$digest" ]]
    cosign sign --yes "$image@$digest"

    if [[ "$state" != draft ]]; then
        gh release create "$version" --repo "$GITHUB_REPOSITORY" --verify-tag --draft --title "$version" \
            --notes "Svincolo for Linux amd64 and arm64."
    fi
    gh release upload "$version" --repo "$GITHUB_REPOSITORY" --clobber ./*
    gh release edit "$version" --repo "$GITHUB_REPOSITORY" --draft=false --latest \
        --notes "Linux amd64 and arm64. Container: \`$image:$version\`. Signed image index: \`$image@$digest\`."
fi

# Also completes a retry that previously published the release but failed here.
skopeo copy --all --preserve-digests "docker://$image:$version" "docker://$image:stable"
