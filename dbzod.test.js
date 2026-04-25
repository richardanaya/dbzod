import assert from "node:assert/strict";
import { execFile } from "node:child_process";
import { readFile } from "node:fs/promises";
import { test } from "node:test";
import { promisify } from "node:util";

const execFileAsync = promisify(execFile);

test("top-level help lists the narrow command surface", async () => {
  const { stdout } = await execFileAsync("node", ["./dbzod.js", "--help"]);

  assert.match(stdout, /dbzod generate \[options\]/);
  assert.match(stdout, /dbzod list tables \[options\]/);
});

test("generate help documents check mode and table filters", async () => {
  const { stdout } = await execFileAsync("node", ["./dbzod.js", "generate", "--help"]);

  assert.match(stdout, /--check\s+Exit nonzero if the output file is stale\./);
  assert.match(stdout, /--table <pattern>/);
  assert.match(stdout, /--exclude-table <pat>/);
});

test("list tables rejects generate-only options", async () => {
  await assert.rejects(
    execFileAsync("node", ["./dbzod.js", "list", "tables", "--check"]),
    /--check is only supported by dbzod generate/,
  );
});

test("generate requires a connection string before doing database work", async () => {
  await assert.rejects(
    execFileAsync("node", ["./dbzod.js", "generate"], { env: { ...process.env, DATABASE_URL: "" } }),
    /Missing PostgreSQL connection string/,
  );
});

test("package metadata targets Zod v4", async () => {
  const packageJson = JSON.parse(await readFile("./package.json", "utf8"));

  assert.equal(packageJson.peerDependencies.zod, "^4.0.0");
  assert.deepEqual(packageJson.files, ["dbzod.js", "dbzod.test.js", "README.md", "LICENSE"]);
});
