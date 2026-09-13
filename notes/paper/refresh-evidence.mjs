import { createHash } from 'node:crypto';
import { readFile, writeFile } from 'node:fs/promises';
import { dirname, relative, resolve } from 'node:path';
import { fileURLToPath } from 'node:url';

const paper = dirname(fileURLToPath(import.meta.url));
const root = resolve(paper, '../..');
const identities = [];
const read = async (path) => {
  const bytes = await readFile(resolve(root, path));
  identities.push({ path, bytes: bytes.length, sha256: createHash('sha256').update(bytes).digest('hex') });
  return JSON.parse(bytes);
};
const pbp = 'artifacts/imputation/20260913-pbp-imputed-v1';
const release = await read('artifacts/imputation/release-candidate-v2/manifest.json');
for (const identity of release.evidence) {
  const path = relative(root, identity.path);
  const bytes = await readFile(resolve(root, path));
  if (bytes.length !== identity.bytes || createHash('sha256').update(bytes).digest('hex') !== identity.sha256) {
    throw new Error(`Release evidence changed: ${path}`);
  }
}
const manifest = await read(`${pbp}/manifest.json`);
const registry = await read(`${pbp}/completion_registry.json`);
const fields = await read(`${pbp}/field_registry.json`);
const grouped = await read(`${pbp}/validation/grouped/coverage_breakdown.json`);
const candidate = await read(`${pbp}/validation/candidate/candidate_validation.json`);
const pitch = await read(`${pbp}/validation/pitch/pitch_validation.json`);
const repair = await read(`${pbp}/geometry_v2_validation.json`);
const context = await read('artifacts/imputation/20260913-context-holdout-v1/report.json');
const legacy = await read('artifacts/imputation/legacy-publication-candidate-v1/migration_summary.json');
const legacyConsumers = await read('artifacts/imputation/legacy-publication-candidate-v1/legacy-consumer-validation-report.json');
const snapshot = {
  evidence_cutoff: '2026-09-13',
  verification_scope: 'Evidence-file identities and extracted results; no new fits, database build, or production publication.',
  code_commit: release.code_commit,
  source_database: candidate.source_identity,
  release_status: {
    status: release.status,
    production_promoted: release.production_promoted,
    publicly_published: release.publicly_published,
  },
  population: release.population,
  registry: { classified_columns: fields.fields.length, source_relations: fields.selected_relations.length, targets: registry.targets.length },
  components: Object.fromEntries(Object.entries(manifest.artifacts).filter(([, value]) => Number.isInteger(value.rows)).map(([key, value]) => [key, { rows: value.rows, data: value.data }])),
  candidate_ready: candidate.candidate_ready,
  candidate_checks: Object.fromEntries(Object.entries(candidate.checks).filter(([key]) => key !== 'source_identity').map(([key, value]) => [key, value.passed])),
  candidate_reconciliation: candidate.checks.reconciliation.value,
  namespace_check: candidate.namespace_check,
  grouped_coverage: {
    fields: grouped.fields.length,
    full_population_match: grouped.full_population_match,
    baseline_mismatches: grouped.reconciliation.filter((row) => !row.baseline_match).length,
    grouping_mismatches: grouped.reconciliation.filter((row) => !row.grouping_dimensions_match).length,
  },
  pitch: Object.fromEntries(Object.entries(pitch).filter(([key]) => !['examples', 'seed_path', 'input_path'].includes(key))),
  geometry_repair: repair,
  context_holdout: context,
  legacy: {
    pointers: legacy.pointers.map(({ model_name, candidate_artifact_id, status, evidence }) => ({ model_name, candidate_artifact_id, status, evidence })),
    consumer_report: legacyConsumers,
  },
  report_identities: identities,
};
if (!snapshot.candidate_ready || snapshot.registry.targets !== snapshot.population.target_fields || !snapshot.grouped_coverage.full_population_match) {
  throw new Error('Candidate evidence is incomplete or inconsistent');
}
await writeFile(resolve(paper, 'evidence-snapshot-2026-09-13.json'), `${JSON.stringify(snapshot, null, 2)}\n`);
console.info(`Extracted ${identities.length} source evidence files; verified ${release.evidence.length} release evidence-file bindings.`);
