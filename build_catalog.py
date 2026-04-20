#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import os
import re
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterable

ANN_RE = re.compile(
    r'@MangaSourceParser\s*\(\s*'
    r'"([^"]+)"\s*,\s*'  # key / name
    r'"([^"]+)"\s*,?\s*'  # title
    r'(?:"([^"]*)"\s*,?\s*)?'  # locale optional
    r'(?:ContentType\.([A-Z_]+)\s*,?\s*)?'  # content type optional
    r'\)',
    re.DOTALL,
)
BROKEN_RE = re.compile(r'@Broken(?:\(\s*"([^"]*)"\s*\))?')
CLASS_RE = re.compile(r'class\s+\w+\([^)]*\)\s*:\s*([A-Za-z0-9_]+)')
SOURCE_DOMAIN_RE = re.compile(r'MangaParserSource\.[A-Z0-9_]+\s*,\s*"([^"]+)"')
CFG_DOMAIN_BLOCK_RE = re.compile(r'ConfigKey\.Domain\((.*?)\)', re.DOTALL)
STRING_RE = re.compile(r'"([^"\\]*(?:\\.[^"\\]*)*)"')


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description='Build public/data/sources.json from YakaTeam/kotatsu-parsers parser sources.'
    )
    parser.add_argument(
        '--repo-dir',
        default=os.environ.get('KOTATSU_REPO_DIR', '.cache/kotatsu-parsers'),
        help='Local path to a checked out kotatsu-parsers repository.',
    )
    parser.add_argument(
        '--output',
        default=os.environ.get('OUTPUT_PATH', 'public/data/sources.json'),
        help='Where to write the generated dataset.',
    )
    parser.add_argument('--owner', default=os.environ.get('KOTATSU_OWNER', 'YakaTeam'))
    parser.add_argument('--repo', default=os.environ.get('KOTATSU_REPO', 'kotatsu-parsers'))
    parser.add_argument('--branch', default=os.environ.get('KOTATSU_BRANCH', 'master'))
    return parser.parse_args()


def parse_strings(fragment: str) -> list[str]:
    return [match.group(1) for match in STRING_RE.finditer(fragment)]


def unique(values: Iterable[str]) -> list[str]:
    seen: set[str] = set()
    output: list[str] = []
    for value in values:
        normalized = value.strip().lower()
        if (
            not normalized
            or normalized in seen
            or '.' not in normalized
            or normalized.endswith('.kt')
            or 'org.koitharu' in normalized
        ):
            continue
        seen.add(normalized)
        output.append(normalized)
    return output


def extract_domains(text: str) -> list[str]:
    domains = SOURCE_DOMAIN_RE.findall(text)
    for match in CFG_DOMAIN_BLOCK_RE.finditer(text):
        domains.extend(parse_strings(match.group(1)))
    return unique(domains)


def extract_entry(repo_root: Path, file_path: Path, owner: str, repo: str, branch: str):
    text = file_path.read_text(encoding='utf-8', errors='replace')
    ann = ANN_RE.search(text)
    if not ann:
        return None

    key, title, locale, content_type = ann.groups()
    language = locale or 'multi'
    content_type = content_type or 'MANGA'
    engine_match = CLASS_RE.search(text)
    engine = engine_match.group(1) if engine_match else None

    broken_match = BROKEN_RE.search(text)
    broken_reason = (broken_match.group(1) or '').strip() if broken_match else ''
    is_broken = broken_match is not None
    domains = extract_domains(text)

    relative_path = file_path.relative_to(repo_root).as_posix()
    health_status = 'broken' if is_broken else 'working'
    health_reason = (
        broken_reason or 'Marked @Broken in upstream parser source.'
        if is_broken
        else 'Not marked @Broken upstream. This is catalog metadata, not a live network test.'
    )

    return {
        'id': key,
        'key': key,
        'title': title,
        'language': language,
        'engine': engine,
        'contentType': content_type,
        'broken': is_broken,
        'brokenReason': broken_reason or None,
        'path': relative_path,
        'repoUrl': f'https://github.com/{owner}/{repo}/blob/{branch}/{relative_path}',
        'rawUrl': f'https://raw.githubusercontent.com/{owner}/{repo}/{branch}/{relative_path}',
        'domains': domains,
        'health': {
            'status': health_status,
            'checkedAt': None,
            'latencyMs': None,
            'httpStatus': None,
            'finalUrl': None,
            'reason': health_reason,
        },
    }


def build_dataset(repo_root: Path, output_path: Path, owner: str, repo: str, branch: str):
    src_root = repo_root / 'src' / 'main' / 'kotlin'
    if not src_root.exists():
        raise SystemExit(f'Parser source root not found: {src_root}')

    entries = []
    seen_keys: set[str] = set()
    duplicates = []

    for file_path in src_root.rglob('*.kt'):
        entry = extract_entry(repo_root, file_path, owner, repo, branch)
        if not entry:
            continue
        if entry['key'] in seen_keys:
            duplicates.append(entry['key'])
            continue
        seen_keys.add(entry['key'])
        entries.append(entry)

    entries.sort(key=lambda item: item['title'].lower())

    summary = {
        'total': len(entries),
        'working': sum(1 for item in entries if item['health']['status'] == 'working'),
        'broken': sum(1 for item in entries if item['health']['status'] == 'broken'),
        'blocked': 0,
        'unknown': 0,
    }

    by_type: dict[str, int] = {}
    by_locale: dict[str, int] = {}
    for item in entries:
        by_type[item['contentType']] = by_type.get(item['contentType'], 0) + 1
        by_locale[item['language']] = by_locale.get(item['language'], 0) + 1

    payload = {
        'generatedAt': datetime.now(timezone.utc).isoformat(),
        'generatedBy': 'scripts/build_catalog.py',
        'sourceRepo': {
            'owner': owner,
            'repo': repo,
            'branch': branch,
        },
        'summary': summary,
        'byType': by_type,
        'byLocale': by_locale,
        'sources': entries,
        'disclaimer': (
            'Generated from YakaTeam parser source annotations, modeled on docs/build_catalog.py. '
            '“Working” means the parser is not marked @Broken upstream. It is not a live site health check.'
        ),
        'duplicatesSkipped': duplicates,
    }

    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(json.dumps(payload, indent=2, ensure_ascii=False) + '\n', encoding='utf-8')
    print(f'wrote {output_path} :: {summary["total"]} sources, {summary["broken"]} broken')


def main() -> None:
    args = parse_args()
    repo_root = Path(args.repo_dir).resolve()
    output_path = Path(args.output).resolve()
    build_dataset(repo_root, output_path, args.owner, args.repo, args.branch)


if __name__ == '__main__':
    main()
