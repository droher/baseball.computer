import { mkdtemp, readFile, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { dirname, resolve } from "node:path";
import { fileURLToPath } from "node:url";
import { spawnSync } from "node:child_process";

const paperDirectory = dirname(fileURLToPath(import.meta.url));
const repositoryRoot = resolve(paperDirectory, "../..");
const sectionsDirectory = resolve(paperDirectory, "sections");
const outputMarkdown = resolve(paperDirectory, "data-coverage-paper.md");
const outputPdf = resolve(paperDirectory, "data-coverage-paper.pdf");
const sectionNames = [
  "00-header.md",
  "01-intro.md",
  "02-data.md",
  "03-taxonomy.md",
  "04-models.md",
  "05-mnar.md",
  "06-deep.md",
  "07-results.md",
  "08-unidentifiable.md",
  "09-publication.md",
  "10-related.md",
  "11-limitations.md",
  "12-reproducibility.md",
  "13-references.md",
];

const normalize = (value) => value
  .replace(/<!--[\s\S]*?-->/g, "")
  .replace(/[\u2010-\u2015\u2212\uFE58\uFE63\uFF0D]/g, "-")
  .replace(/[ \t]+\n/g, "\n")
  .replace(/\n{3,}/g, "\n\n")
  .trim();

const parseMetadata = (value) => {
  const match = value.match(/^---\n([\s\S]*?)\n---\n*/);
  if (!match) {
    throw new Error("sections/00-header.md must begin with YAML metadata");
  }
  const metadata = Object.fromEntries(
    match[1]
      .split("\n")
      .map((line) => line.match(/^([^:]+):\s*[\"']?(.*?)[\"']?\s*$/))
      .filter(Boolean)
      .map(([, key, value]) => [key, value]),
  );
  for (const field of ["title", "subtitle", "date"]) {
    if (!metadata[field]) {
      throw new Error(`missing YAML metadata field: ${field}`);
    }
  }
  return { metadata, body: value.slice(match[0].length) };
};

const run = (command, arguments_) => {
  const result = spawnSync(command, arguments_, {
    cwd: repositoryRoot,
    encoding: "utf8",
  });
  if (result.status !== 0) {
    throw new Error(`${command} failed:\n${result.stderr || result.stdout}`);
  }
  if (result.stderr) {
    process.stderr.write(result.stderr);
  }
};

const typstContent = (value) => value
  .replace(/\\/g, "\\\\")
  .replace(/[\[\]]/g, (character) => `\\${character}`);

const temporaryDirectory = await mkdtemp(resolve(tmpdir(), "baseball-computer-paper-"));
const temporaryBase = resolve(temporaryDirectory, "paper");

try {
  const sources = await Promise.all(
    sectionNames.map(async (name) => [name, await readFile(resolve(sectionsDirectory, name), "utf8")]),
  );
  const { metadata, body: headerBody } = parseMetadata(sources[0][1]);
  const assembledSections = [normalize(headerBody)];

  for (const [index, [, source]] of sources.slice(1, 13).entries()) {
    const number = index + 1;
    const section = normalize(source).replace(/^##\s+(.+)$/m, `## ${number}. $1`);
    if (!section.startsWith(`## ${number}. `)) {
      throw new Error(`section ${number} has no second-level Markdown heading`);
    }
    assembledSections.push(section);
  }

  assembledSections.push(normalize(sources[13][1]));

  const metadataBlock = [
    "---",
    `title: \"${metadata.title}\"`,
    `subtitle: \"${metadata.subtitle}\"`,
    `date: \"${metadata.date}\"`,
    "---",
  ].join("\n");
  const assembledMarkdown = `${metadataBlock}\n\n${assembledSections.join("\n\n")}\n`;

  await writeFile(outputMarkdown, assembledMarkdown);
  const pandocMarkdown = assembledMarkdown.replace(/`([^`]+)`/g, (_, value) =>
    `\`${value.replace(/_/g, "_\u200B")}\``,
  );
  await writeFile(`${temporaryBase}.md`, pandocMarkdown);
  run("pandoc", [
    "--from=markdown+tex_math_dollars",
    "--to=typst",
    `${temporaryBase}.md`,
    "--output",
    `${temporaryBase}.body.typ`,
  ]);

  const [style, rawBody] = await Promise.all([
    readFile(resolve(paperDirectory, "paper-style.typ"), "utf8"),
    readFile(`${temporaryBase}.body.typ`, "utf8"),
  ]);
  const body = rawBody.replace(
    "== References\n<references>",
    "#pagebreak()\n== References\n<references>\n#set text(size: 9pt)\n#set par(leading: 0.5em, first-line-indent: 0pt)",
  );
  const document = [
    `#let manuscript-title = [${typstContent(metadata.title)}]`,
    `#let manuscript-subtitle = [${typstContent(metadata.subtitle)}]`,
    `#let manuscript-date = [${typstContent(metadata.date)}]`,
    style,
    body,
  ].join("\n\n");
  await writeFile(`${temporaryBase}.typ`, document);
  run("typst", [
    "compile",
    "--root",
    temporaryDirectory,
    `${temporaryBase}.typ`,
    outputPdf,
  ]);
  console.log(`Built ${outputMarkdown} and ${outputPdf}`);
} finally {
  await rm(temporaryDirectory, { recursive: true, force: true });
}
