#!/usr/bin/env bash
# ─────────────────────────────────────────────────────────────────────
# split-push.sh — split a monorepo folder into its own branch and push
#                 to an external repo (for per-language SDK repos).
#
# Uses `git subtree split` which preserves the full commit history of
# the subdirectory (rewritten so that folder becomes the repo root).
#
# PREREQUISITES:
#   - git (with subtree support, standard since git 1.7.11)
#   - gh (GitHub CLI) — only needed for --pr
#   - SSH key or token with push access to the target repo
#
# USAGE:
#   scripts/split-push.sh <folder> [options]
#   scripts/split-push.sh all   [options]
#   scripts/split-push.sh --list
#   scripts/split-push.sh --check
#
# OPTIONS:
#   -b, --branch <name>   Target branch on the remote (default: main)
#   --base <branch>       PR base branch (default: main)
#   --pr <title>          Create a PR after pushing (implies -b <auto>)
#   --force               Force-push (overwrite remote branch history)
#   --squash              Squash all commits into one before pushing
#   --dry-run             Show what would happen without pushing
#   --list                List configured folders + remotes
#   --check               Verify all configured folders exist locally
#   -h, --help            Show this help
#
# EXAMPLES:
#   # Push bindings/go to its repo's main branch
#   scripts/split-push.sh bindings/go
#
#   # Push bindings/python to a feature branch (for a PR)
#   scripts/split-push.sh bindings/python -b fix/reconnect
#
#   # Push + auto-create PR on GitHub
#   scripts/split-push.sh bindings/go --pr "Fix topic validation"
#
#   # Push all configured folders to their repos
#   scripts/split-push.sh all
#
#   # See what would happen
#   scripts/split-push.sh bindings/go --dry-run
#
#   # Push server only, squashed (clean single-commit history)
#   scripts/split-push.sh quic-server --squash
#
# CONFIG: scripts/split-repos.conf
# ─────────────────────────────────────────────────────────────────────

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
CONF_FILE="$SCRIPT_DIR/split-repos.conf"

# Colors
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[0;33m'
CYAN='\033[0;36m'
BOLD='\033[1m'
NC='\033[0m'

c_print()  { echo -e "${CYAN}$*${NC}"; }
ok_print() { echo -e "${GREEN}✓${NC} $*"; }
w_print()  { echo -e "${YELLOW}⚠${NC} $*"; }
e_print()  { echo -e "${RED}✗${NC} $*" >&2; }

# ── Parse args ────────────────────────────────────────────────────────

FOLDER=""
TARGET_BRANCH="main"
BASE_BRANCH="main"
PR_TITLE=""
FORCE=false
SQUASH=false
DRY_RUN=false
ACTION="push"

while [[ $# -gt 0 ]]; do
    case "$1" in
        -b|--branch)    TARGET_BRANCH="$2"; shift 2 ;;
        --base)         BASE_BRANCH="$2"; shift 2 ;;
        --pr)           PR_TITLE="$2"; ACTION="pr"; shift 2 ;;
        --force)        FORCE=true; shift ;;
        --squash)       SQUASH=true; shift ;;
        --dry-run)      DRY_RUN=true; shift ;;
        --list)         ACTION="list"; shift ;;
        --check)        ACTION="check"; shift ;;
        -h|--help)      ACTION="help"; shift ;;
        --)             shift; break ;;
        -*)             e_print "Unknown option: $1"; exit 1 ;;
        *)              FOLDER="$1"; shift ;;
    esac
done

help_text() {
    sed -n '3,/^# ───.*────────/p' "$0" | sed 's/^# \?//' | head -n -1
}

if [[ "$ACTION" == "help" ]]; then
    help_text
    exit 0
fi

# ── Read config file ──────────────────────────────────────────────────

# Returns: folder\tremote pairs (tab-separated)
read_config() {
    grep -v '^\s*#' "$CONF_FILE" | grep -v '^\s*$' | awk '{print $1 "\t" $2}'
}

# folder_to_branch_name <folder>
# bindings/go → split/bindings-go
folder_to_branch() {
    echo "split/$(echo "$1" | tr '/' '-')"
}

# folder_to_remote <folder>
folder_to_remote() {
    read_config | awk -F'\t' -v f="$1" '$1 == f {print $2; exit}'
}

if [[ "$ACTION" == "list" ]]; then
    echo -e "${BOLD}Configured split repos:${NC}"
    echo
    while IFS=$'\t' read -r folder remote; do
        if [[ -d "$REPO_ROOT/$folder" ]]; then
            exists="${GREEN}✓${NC}"
        else
            exists="${RED}✗ (missing)${NC}"
        fi
        printf "  %b %-20s → %s\n" "$exists" "$folder" "$remote"
    done < <(read_config)
    echo
    echo "Config: $CONF_FILE"
    exit 0
fi

if [[ "$ACTION" == "check" ]]; then
    echo -e "${BOLD}Checking configured folders:${NC}"
    echo
    all_ok=true
    while IFS=$'\t' read -r folder remote; do
        if [[ -d "$REPO_ROOT/$folder" ]]; then
            ok_print "$folder — exists"
        else
            e_print "$folder — MISSING"
            all_ok=false
        fi
    done < <(read_config)
    echo
    if $all_ok; then
        ok_print "All folders present."
    else
        e_print "Some folders are missing."
        exit 1
    fi
    exit 0
fi

if [[ -z "$FOLDER" ]]; then
    e_print "No folder specified."
    echo "Usage: scripts/split-push.sh <folder|all> [options]"
    echo "       scripts/split-push.sh --list"
    echo "       scripts/split-push.sh --check"
    exit 1
fi

# ── Collect folders to process ────────────────────────────────────────

if [[ "$FOLDER" == "all" ]]; then
    FOLDERS=()
    while IFS=$'\t' read -r f r; do
        FOLDERS+=("$f|$r")
    done < <(read_config)
else
    REMOTE="$(folder_to_remote "$FOLDER")"
    if [[ -z "$REMOTE" ]]; then
        e_print "Folder '$FOLDER' not found in $CONF_FILE"
        echo ""
        echo "Configured folders:"
        read_config | awk -F'\t' '{print "  " $1}'
        exit 1
    fi
    FOLDERS=("$FOLDER|$REMOTE")
fi

# ── Core push function ────────────────────────────────────────────────

do_split_push() {
    local folder="$1"
    local remote="$2"
    local local_branch
    local_branch="$(folder_to_branch "$folder")"

    echo ""
    c_print "${BOLD}── $folder ──${NC}"
    c_print "remote: $remote"
    c_print "branch: $TARGET_BRANCH"

    # Verify folder exists
    if [[ ! -d "$REPO_ROOT/$folder" ]]; then
        e_print "Folder '$folder' does not exist locally."
        return 1
    fi

    # Clean up any stale split branch
    git -C "$REPO_ROOT" branch -D "$local_branch" 2>/dev/null || true

    if $DRY_RUN; then
        w_print "[dry-run] would: git subtree split --prefix=$folder -b $local_branch"
        if $SQUASH; then
            w_print "[dry-run] would: squash into single commit"
        fi
        w_print "[dry-run] would: git push $remote $local_branch:$TARGET_BRANCH"
        if [[ "$ACTION" == "pr" ]]; then
            w_print "[dry-run] would: gh pr create --repo $remote --base $BASE_BRANCH"
        fi
        return 0
    fi

    # ── Step 1: Split ─────────────────────────────────────────────────
    c_print "splitting --prefix=$folder ..."

    if ! git -C "$REPO_ROOT" subtree split --prefix="$folder" -b "$local_branch" 2>/dev/null; then
        e_print "git subtree split failed for '$folder'"
        return 1
    fi
    ok_print "split complete → $local_branch"

    # ── Step 2 (optional): Squash ─────────────────────────────────────
    if $SQUASH; then
        c_print "squashing into single commit ..."
        # Create an orphan branch with a single squashed commit
        local squash_msg="Export $folder from Vireon monorepo

Generated by scripts/split-push.sh on $(date -u +%Y-%m-%d)"
        git -C "$REPO_ROOT" checkout --orphan "${local_branch}-sq" "$local_branch"
        git -C "$REPO_ROOT" commit -m "$squash_msg"
        git -C "$REPO_ROOT" branch -D "$local_branch"
        git -C "$REPO_ROOT" branch -m "$local_branch"
        ok_print "squashed to single commit"
    fi

    # ── Step 3: Push ──────────────────────────────────────────────────
    local push_flag=""
    $FORCE && push_flag="--force"

    c_print "pushing to $remote:$TARGET_BRANCH ..."
    if ! git -C "$REPO_ROOT" push $push_flag "$remote" "$local_branch:$TARGET_BRANCH"; then
        e_print "push failed."
        w_print "If the remote has commits not in this split, try --force"
        w_print "If the remote repo doesn't exist, create it first on GitHub."
        git -C "$REPO_ROOT" branch -D "$local_branch" 2>/dev/null || true
        return 1
    fi
    ok_print "pushed to $TARGET_BRANCH"

    # ── Step 4 (optional): Create PR ──────────────────────────────────
    if [[ "$ACTION" == "pr" ]]; then
        if ! command -v gh &>/dev/null; then
            e_print "'gh' CLI not found — skipping PR creation."
            w_print "Create PR manually at: $remote"
        else
            c_print "creating PR ..."
            # Extract owner/repo from URL for --repo flag
            local pr_repo
            pr_repo=$(echo "$remote" \
                | sed 's|.*github.com[:/]||' \
                | sed 's|\.git$||')
            if gh pr create \
                --repo "$pr_repo" \
                --base "$BASE_BRANCH" \
                --head "$TARGET_BRANCH" \
                --title "$PR_TITLE" \
                --body "Auto-exported from Vireon monorepo by \`scripts/split-push.sh\`."; then
                ok_print "PR created: $PR_TITLE"
            else
                e_print "PR creation failed. Push succeeded — create PR manually."
            fi
        fi
    fi

    # ── Cleanup ───────────────────────────────────────────────────────
    git -C "$REPO_ROOT" branch -D "$local_branch" 2>/dev/null || true
}

# ── Main ──────────────────────────────────────────────────────────────

echo -e "${BOLD}Vireon split-push${NC} — folder → external repo"

cd "$REPO_ROOT"

# Ensure working tree is clean (for subtree split)
if ! git diff --quiet || ! git diff --cached --quiet; then
    e_print "Working tree has uncommitted changes."
    w_print "Commit or stash first — subtree split needs a clean tree."
    exit 1
fi

FAILED=()
SUCCEEDED=()

for entry in "${FOLDERS[@]}"; do
    IFS='|' read -r folder remote <<< "$entry"
    if do_split_push "$folder" "$remote"; then
        SUCCEEDED+=("$folder")
    else
        FAILED+=("$folder")
    fi
done

# ── Summary ───────────────────────────────────────────────────────────

echo ""
echo -e "${BOLD}──────────────────────────────────${NC}"
if [[ ${#SUCCEEDED[@]} -gt 0 ]]; then
    ok_print "Pushed (${#SUCCEEDED[@]}):"
    for f in "${SUCCEEDED[@]}"; do
        echo "    $f"
    done
fi
if [[ ${#FAILED[@]} -gt 0 ]]; then
    e_print "Failed (${#FAILED[@]}):"
    for f in "${FAILED[@]}"; do
        echo "    $f"
    done
    exit 1
fi
