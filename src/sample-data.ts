import type { SourceDataset } from '@/types'

const LANGUAGE_NAMES: Record<string, string> = {
  en: 'English',
  de: 'German',
  ru: 'Russian',
  fr: 'French',
  es: 'Spanish',
  it: 'Italian',
  pt: 'Portuguese',
  tr: 'Turkish',
  vi: 'Vietnamese',
  id: 'Indonesian',
  th: 'Thai',
  ar: 'Arabic',
  pl: 'Polish',
  uk: 'Ukrainian',
  ja: 'Japanese',
  ko: 'Korean',
  zh: 'Chinese',
  multi: 'Multiple',
}

function withSearchText<T extends SourceDataset['sources'][number]>(source: T): T {
  const languageName =
    source.languageName || LANGUAGE_NAMES[source.language] || source.language.toUpperCase()

  return {
    ...source,
    languageName,
    searchText: [
      source.title,
      source.key,
      source.language,
      languageName,
      source.engine ?? '',
      source.contentType ?? '',
      source.path,
      source.brokenReason ?? '',
      ...(source.domains ?? []),
    ]
      .join(' ')
      .toLowerCase(),
  }
}

export const sampleData: SourceDataset = {
  generatedAt: null,
  generatedBy: 'src/sample-data.ts',
  sourceRepo: {
    owner: 'YakaTeam',
    repo: 'kotatsu-parsers',
    branch: 'master',
  },
  disclaimer:
    'This is bundled demo data for the catalog UI. Run the generator or GitHub Action to build the full static dataset.',
  summary: {
    total: 3,
    working: 2,
    broken: 1,
    blocked: 0,
    unknown: 0,
    nsfw: 1,
  },
  byLocale: {
    en: 1,
    ru: 1,
    vi: 1,
  },
  byType: {
    MANGA: 3,
  },
  duplicatesSkipped: [],
  sources: [
    {
      id: 'MANGATOWN',
      key: 'MANGATOWN',
      title: 'MangaTown',
      language: 'en',
      languageName: 'English',
      engine: 'PagedMangaParser',
      contentType: 'MANGA',
      brokenReason: null,
      nsfw: false,
      path: 'src/main/kotlin/org/koitharu/kotatsu/parsers/site/en/MangaTownParser.kt',
      repoUrl:
        'https://github.com/YakaTeam/kotatsu-parsers/blob/master/src/main/kotlin/org/koitharu/kotatsu/parsers/site/en/MangaTownParser.kt',
      rawUrl:
        'https://raw.githubusercontent.com/YakaTeam/kotatsu-parsers/master/src/main/kotlin/org/koitharu/kotatsu/parsers/site/en/MangaTownParser.kt',
      domains: ['www.mangatown.com'],
      health: {
        status: 'working',
        checkedAt: null,
        latencyMs: null,
        httpStatus: null,
        finalUrl: null,
        reason: 'Demo entry only. No live network test has been executed.',
      },
    },
    {
      id: 'DESUME',
      key: 'DESUME',
      title: 'Desu',
      language: 'ru',
      languageName: 'Russian',
      engine: 'PagedMangaParser',
      contentType: 'MANGA',
      brokenReason: null,
      nsfw: true,
      path: 'src/main/kotlin/org/koitharu/kotatsu/parsers/site/ru/DesuMeParser.kt',
      repoUrl:
        'https://github.com/YakaTeam/kotatsu-parsers/blob/master/src/main/kotlin/org/koitharu/kotatsu/parsers/site/ru/DesuMeParser.kt',
      rawUrl:
        'https://raw.githubusercontent.com/YakaTeam/kotatsu-parsers/master/src/main/kotlin/org/koitharu/kotatsu/parsers/site/ru/DesuMeParser.kt',
      domains: ['x.desu.city', 'desu.city', 'desu.work', 'desu.store', 'desu.win'],
      health: {
        status: 'working',
        checkedAt: null,
        latencyMs: null,
        httpStatus: null,
        finalUrl: null,
        reason: 'Demo entry only. No live network test has been executed.',
      },
    },
    {
      id: 'OTRUYEN',
      key: 'OTRUYEN',
      title: 'OTruyen',
      language: 'vi',
      languageName: 'Vietnamese',
      engine: 'PagedMangaParser',
      contentType: 'MANGA',
      brokenReason: 'Demo broken entry',
      nsfw: false,
      path: 'src/main/kotlin/org/koitharu/kotatsu/parsers/site/vi/OTruyenParser.kt',
      repoUrl:
        'https://github.com/YakaTeam/kotatsu-parsers/blob/master/src/main/kotlin/org/koitharu/kotatsu/parsers/site/vi/OTruyenParser.kt',
      rawUrl:
        'https://raw.githubusercontent.com/YakaTeam/kotatsu-parsers/master/src/main/kotlin/org/koitharu/kotatsu/parsers/site/vi/OTruyenParser.kt',
      domains: [],
      health: {
        status: 'broken',
        checkedAt: null,
        latencyMs: null,
        httpStatus: null,
        finalUrl: null,
        reason: 'Demo entry only. This parser is marked broken in sample data.',
      },
    },
  ].map(withSearchText),
}
