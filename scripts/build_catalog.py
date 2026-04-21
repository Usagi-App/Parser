#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import os
import re
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterable

ANN_RE = re.compile(
    r'@MangaSourceParser\s*\(\s*'
    r'"([^"]+)"\s*,\s*'
    r'"([^"]+)"\s*,?\s*'
    r'(?:(?:"([^"]*)")\s*,?\s*)?'
    r'(?:ContentType\.([A-Z_]+)\s*,?\s*)?'
    r'\)',
    re.DOTALL,
)

BROKEN_RE = re.compile(r'@Broken(?:\(\s*"([^"]*)"\s*\))?')
CLASS_DECL_RE = re.compile(
    r'\b(?:internal|public|private|protected)?\s*'
    r'(?:abstract|open|sealed|data)?\s*class\s+'
    r'([A-Za-z0-9_]+)'
    r'(?:\s*<[^>{}]*>)?'
    r'(?:\s*\([^)]*\))?'
    r'\s*:\s*([A-Za-z0-9_]+)'
    r'(?:\((.*?)\))?',
    re.DOTALL,
)
CLASS_NAME_RE = re.compile(
    r'\b(?:internal|public|private|protected)?\s*'
    r'(?:abstract|open|sealed|data)?\s*class\s+([A-Za-z0-9_]+)'
)
SOURCE_DOMAIN_EXPR_RE = re.compile(
    r'MangaParserSource\.[A-Z0-9_]+\s*,\s*([^,\n)]+)'
)
CFG_DOMAIN_BLOCK_RE = re.compile(r'ConfigKey\.Domain\((.*?)\)', re.DOTALL)
DOMAIN_PROPERTY_RE = re.compile(
    r'(?:override\s+)?(?:val|var)\s+[A-Za-z0-9_]*domain[A-Za-z0-9_]*\s*=\s*([^\n;]+)',
    re.IGNORECASE,
)
NAMED_DOMAIN_ARG_RE = re.compile(r'\bdomain\s*=\s*([^,\n)]+)', re.IGNORECASE)
CONST_RE = re.compile(
    r'(?:private\s+|protected\s+|internal\s+|public\s+)?'
    r'(?:const\s+)?val\s+([A-Za-z_][A-Za-z0-9_]*)\s*=\s*"([^"]+)"'
)
STRING_RE = re.compile(r'"([^"\\]*(?:\\.[^"\\]*)*)"')
IDENTIFIER_RE = re.compile(r'\b([A-Z][A-Z0-9_]*(?:\.[A-Z][A-Z0-9_]*)*)\b')

LANGUAGE_NAMES: dict[str, str] = {
    'en': 'English',
    'de': 'German',
    'ru': 'Russian',
    'fr': 'French',
    'es': 'Spanish',
    'it': 'Italian',
    'pt': 'Portuguese',
    'tr': 'Turkish',
    'vi': 'Vietnamese',
    'id': 'Indonesian',
    'th': 'Thai',
    'ar': 'Arabic',
    'pl': 'Polish',
    'uk': 'Ukrainian',
    'ja': 'Japanese',
    'ko': 'Korean',
    'zh': 'Chinese',
    'be': 'Belarusian',
    'cs': 'Czech',
    'multi': 'Multiple',
}


@dataclass(slots=True)
class KotlinClassInfo:
    name: str | None
    parent: str | None
    super_args: str
    constants: dict[str, str]
    text: str


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description='Build public/data/sources.json from parser source files.'
    )
    parser.add_argument(
        '--repo-dir',
        default=os.environ.get('KOTATSU_REPO_DIR', '.cache/kotatsu-parsers'),
        help='Local path to a checked out parser repository.',
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


def unique(values: Iterable[str]) -> list[str]:
    seen: set[str] = set()
    output: list[str] = []

    for value in values:
        normalized = value.strip().lower()
        if not is_probable_domain(normalized) or normalized in seen:
            continue
        seen.add(normalized)
        output.append(normalized)

    return output


def is_probable_domain(value: str) -> bool:
    return bool(
        value
        and '.' in value
        and ' ' not in value
        and not value.endswith('.kt')
        and 'org.koitharu' not in value
        and '/' not in value
        and not value.startswith('http')
    )


def parse_quoted_domains(fragment: str) -> list[str]:
    return [
        match.group(1)
        for match in STRING_RE.finditer(fragment)
        if is_probable_domain(match.group(1))
    ]


def resolve_identifier(token: str, constants: dict[str, str]) -> str | None:
    direct = constants.get(token)
    if direct and is_probable_domain(direct):
        return direct

    tail = token.rsplit('.', 1)[-1]
    resolved = constants.get(tail)
    if resolved and is_probable_domain(resolved):
        return resolved

    return None


def parse_domain_candidates(fragment: str, constants: dict[str, str]) -> list[str]:
    domains = parse_quoted_domains(fragment)

    for match in IDENTIFIER_RE.finditer(fragment):
        resolved = resolve_identifier(match.group(1), constants)
        if resolved:
            domains.append(resolved)

    return unique(domains)


def normalize_language_name(language: str) -> str:
    return LANGUAGE_NAMES.get(language, language.upper())


def infer_nsfw(text: str, title: str, path: str, domains: list[str]) -> bool:
    haystack = ' '.join([text, title, path, *domains]).lower()
    nsfw_markers = ['nsfw', '18+', 'adult', 'hentai', 'ecchi', 'porn', 'xxx']
    return any(marker in haystack for marker in nsfw_markers)


def build_search_text(
    *,
    title: str,
    key: str,
    language: str,
    language_name: str,
    engine: str | None,
    content_type: str,
    path: str,
    broken_reason: str | None,
    domains: list[str],
) -> str:
    return ' '.join(
        [
            title,
            key,
            language,
            language_name,
            engine or '',
            content_type or '',
            path,
            broken_reason or '',
            *domains,
        ]
    ).strip().lower()


def parse_class_info(file_path: Path) -> KotlinClassInfo:
    text = file_path.read_text(encoding='utf-8', errors='replace')
    class_match = CLASS_DECL_RE.search(text)
    name_match = CLASS_NAME_RE.search(text)

    constants: dict[str, str] = {}
    for match in CONST_RE.finditer(text):
        constant_value = match.group(2).strip()
        if is_probable_domain(constant_value):
            constants[match.group(1)] = constant_value.lower()

    return KotlinClassInfo(
        name=name_match.group(1) if name_match else None,
        parent=class_match.group(2) if class_match else None,
        super_args=class_match.group(3) if class_match and class_match.group(3) else '',
        constants=constants,
        text=text,
    )


def extract_domains_from_text(text: str, constants: dict[str, str], super_args: str = '') -> list[str]:
    domains: list[str] = []

    for match in SOURCE_DOMAIN_EXPR_RE.finditer(text):
        domains.extend(parse_domain_candidates(match.group(1), constants))

    for match in CFG_DOMAIN_BLOCK_RE.finditer(text):
        domains.extend(parse_domain_candidates(match.group(1), constants))

    for match in DOMAIN_PROPERTY_RE.finditer(text):
        domains.extend(parse_domain_candidates(match.group(1), constants))

    for match in NAMED_DOMAIN_ARG_RE.finditer(text):
        domains.extend(parse_domain_candidates(match.group(1), constants))

    if super_args:
        domains.extend(parse_domain_candidates(super_args, constants))

    return unique(domains)


def collect_domains(
    class_name: str | None,
    class_index: dict[str, KotlinClassInfo],
    seen: set[str] | None = None,
) -> tuple[list[str], dict[str, str]]:
    if not class_name:
        return [], {}

    if seen is None:
        seen = set()

    if class_name in seen:
        return [], {}

    seen.add(class_name)
    info = class_index.get(class_name)
    if not info:
        return [], {}

    parent_domains, parent_constants = collect_domains(info.parent, class_index, seen)
    merged_constants = {**parent_constants, **info.constants}
    own_domains = extract_domains_from_text(info.text, merged_constants, info.super_args)

    return unique([*own_domains, *parent_domains]), merged_constants


def extract_entry(
    repo_root: Path,
    file_path: Path,
    owner: str,
    repo: str,
    branch: str,
    class_index: dict[str, KotlinClassInfo],
):
    info = parse_class_info(file_path)
    text = info.text
    ann = ANN_RE.search(text)
    if not ann:
        return None

    key, title, locale, content_type = ann.groups()
    language = locale or 'multi'
    language_name = normalize_language_name(language)
    content_type = content_type or 'MANGA'

    engine = info.parent
    if not engine or engine == 'AbstractMangaParser':
        engine = info.name or 'Custom'

    broken_match = BROKEN_RE.search(text)
    broken_reason = (broken_match.group(1) or '').strip() if broken_match else ''
    is_broken = broken_match is not None

    domains, _ = collect_domains(info.name, class_index)
    relative_path = file_path.relative_to(repo_root).as_posix()
    is_nsfw = infer_nsfw(text, title, relative_path, domains)

    health_status = 'broken' if is_broken else 'working'
    effective_broken_reason = (
        broken_reason or 'Marked @Broken upstream (no reason provided)'
        if is_broken
        else None
    )
    health_reason = (
        effective_broken_reason
        if is_broken
        else 'Catalog metadata only. No live site health check has been executed.'
    )

    return {
        'id': key,
        'key': key,
        'title': title,
        'language': language,
        'languageName': language_name,
        'engine': engine,
        'contentType': content_type,
        'broken': is_broken,
        'brokenReason': effective_broken_reason,
        'nsfw': is_nsfw,
        'path': relative_path,
        'repoUrl': f'https://github.com/{owner}/{repo}/blob/{branch}/{relative_path}',
        'rawUrl': f'https://raw.githubusercontent.com/{owner}/{repo}/{branch}/{relative_path}',
        'domains': domains,
        'searchText': build_search_text(
            title=title,
            key=key,
            language=language,
            language_name=language_name,
            engine=engine,
            content_type=content_type,
            path=relative_path,
            broken_reason=effective_broken_reason,
            domains=domains,
        ),
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

    kotlin_files = sorted(src_root.rglob('*.kt'))
    class_index: dict[str, KotlinClassInfo] = {}

    for file_path in kotlin_files:
        info = parse_class_info(file_path)
        if info.name and info.name not in class_index:
            class_index[info.name] = info

    entries = []
    seen_keys: set[str] = set()
    duplicates: list[str] = []

    for file_path in kotlin_files:
        entry = extract_entry(repo_root, file_path, owner, repo, branch, class_index)
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
        'nsfw': sum(1 for item in entries if item.get('nsfw')),
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
            'This website is an informational catalog of parser source metadata. '
            '“Working” means the parser is not marked @Broken in the upstream source code. '
            'It is not a live site health check.'
        ),
        'duplicatesSkipped': duplicates,
    }

    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(
        json.dumps(payload, indent=2, ensure_ascii=False) + '\n',
        encoding='utf-8',
    )

    print(
        f'wrote {output_path} :: {summary["total"]} sources, '
        f'{summary["broken"]} broken, {summary["nsfw"]} nsfw'
    )


def main() -> None:
    args = parse_args()
    repo_root = Path(args.repo_dir).resolve()
    output_path = Path(args.output).resolve()
    build_dataset(repo_root, output_path, args.owner, args.repo, args.branch)


if __name__ == '__main__':
    main()
