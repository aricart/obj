import { cli } from "@aricart/cobra";
import type { Flags } from "@aricart/cobra";
import { getSha256Backend, Objm, setSha256Backend } from "@nats-io/obj";
import type { ObjectStoreOptions } from "@nats-io/obj";
import { createHash } from "node:crypto";
import { createReadStream, createWriteStream } from "node:fs";
import { readFile, stat } from "node:fs/promises";
import process from "node:process";
import { Readable, Writable } from "node:stream";
import { jetstream, jetstreamManager } from "@nats-io/jetstream";
import { startFastIngest } from "@nats-io/jetstream/internal";
import { nuid } from "@nats-io/nats-core";
import type { ConnectionOptions, NatsConnection } from "@nats-io/nats-core";

const root = cli({
  use: "obj",
});

// node apis that deno, node and bun all have, as web streams
function readStream(path: string): ReadableStream<Uint8Array> {
  return Readable.toWeb(createReadStream(path)) as ReadableStream<Uint8Array>;
}

function writeStream(path: string): WritableStream<Uint8Array> {
  return Writable.toWeb(createWriteStream(path)) as WritableStream<Uint8Array>;
}

// deno has its own transport, node and bun use the node one. the name is
// picked at runtime, so each runtime only resolves the one it uses
async function connectTo(servers: string): Promise<NatsConnection> {
  const transport = "Deno" in globalThis
    ? "@nats-io/transport-deno"
    : "@nats-io/transport-node";
  const { connect } = await import(transport) as {
    connect: (opts: ConnectionOptions) => Promise<NatsConnection>;
  };
  return connect({ servers });
}

async function createConnection(flags: Flags) {
  const sha = flags.value<string>("sha");
  if (sha !== "auto" && sha !== "js" && sha !== "native") {
    throw new Error(`--sha must be auto, js or native`);
  }
  // auto keeps the library's default - native where the runtime has it, in
  // versions that have auto, js before that
  if (sha !== "auto") {
    setSha256Backend(sha);
  }
  const nc = await connectTo(flags.value<string>("server"));
  console.log(
    `connected ${nc.info?.server_name} ${nc.info?.version} api_lvl ${
      nc.info?.api_lvl ?? 0
    } sha ${getSha256Backend()}${sha === "auto" ? " (auto)" : ""}`,
  );
  return { nc, objm: new Objm(nc) };
}

// sha256 of a local file, base64url without padding
async function hashFile(path: string): Promise<string> {
  const h = createHash("sha256");
  for await (const chunk of createReadStream(path)) {
    h.update(chunk);
  }
  return h.digest("base64url");
}

// compares the local file to the object's size and digest, returns false on mismatch
async function verify(
  path: string,
  info: { size: number; digest: string },
): Promise<boolean> {
  const { size } = await stat(path);
  const local = await hashFile(path);
  // stored as SHA-256=<base64url padded>; older clients may have used base64
  const stored = info.digest.replace(/^SHA-256=/, "").replaceAll("+", "-")
    .replaceAll("/", "_").replace(/=+$/, "");
  const ok = size === info.size && local === stored;
  console.log(
    ok
      ? `verified: size ${size} sha256 ${local}`
      : `MISMATCH: local size ${size} sha256 ${local}, object size ${info.size} digest ${info.digest}`,
  );
  return ok;
}

// parses 10, 10k, 10m, 10g (or kib, mib, gib)
function parseSize(s: string): number {
  const a = /^(\d+)(\D+)?$/.exec(s);
  if (a === null) {
    throw new Error(`invalid size: ${s}`);
  }
  const n = parseInt(a[1]);
  switch (a[2]?.toLowerCase()) {
    case undefined:
      return n;
    case "k":
    case "kib":
      return n * 1024;
    case "m":
    case "mib":
      return n * 1024 * 1024;
    case "g":
    case "gib":
      return n * 1024 * 1024 * 1024;
    default:
      throw new Error(`invalid size: ${s}`);
  }
}

// allowBatched is a hidden obj option:
// - fast: fast ingest; creates a new bucket with allow_batched (2.14+), and
//   rejects an existing bucket without it
// - acked: per-chunk acks
function modeOpts(mode: string): Record<string, boolean> {
  switch (mode) {
    case "acked":
      return {};
    case "fast":
      return { allowBatched: true };
    default:
      throw new Error(`--mode must be acked or fast`);
  }
}

// the put path the handle resolved to
function putPath(os: unknown): string {
  return (os as { allowBatched: boolean }).allowBatched
    ? "fast ingest"
    : "acked";
}

function rate(bytes: number, ms: number): string {
  const mib = bytes / (1024 * 1024);
  return `${mib.toFixed(1)} MiB in ${(ms / 1000).toFixed(2)}s (${
    (mib / (ms / 1000)).toFixed(1)
  } MiB/s)`;
}

const put = root.addCommand({
  use: "put --file filepath",
  short: "put a file into the object store",
  run: async (_cmd, _args, flags) => {
    flags.checkRequired();
    const { nc, objm } = await createConnection(flags);
    const bucket = flags.value<string>("bucket");
    const storage = flags.value<string>("storage");
    if (storage !== "file" && storage !== "memory") {
      throw new Error(`--storage must be file or memory`);
    }
    const os = await objm.create(bucket, {
      storage,
      compression: flags.value<boolean>("compression"),
      ...modeOpts(flags.value<string>("mode")),
    } as unknown as Partial<ObjectStoreOptions>);
    const status = await os.status();
    console.log(
      `bucket ${bucket} storage ${status.storage} put path ${putPath(os)}`,
    );

    const path = flags.value<string>("file");
    const name = flags.value<string>("name") || path;
    const { size } = await stat(path);
    const start = performance.now();
    const oi = await os.put({ name }, readStream(path));
    const ms = performance.now() - start;
    console.log(`put ${name}: ${rate(size, ms)} chunks ${oi.chunks}`);

    // check what the server stored, not just what put returned
    const stored = await os.info(name);
    const ok = stored !== null && await verify(path, stored);
    await nc.close();
    return ok ? 0 : 1;
  },
});

const compare = root.addCommand({
  use: "compare --file filepath [--runs 3]",
  short: "put the same file with per-chunk acks and fast ingest",
  run: async (_cmd, _args, flags) => {
    flags.checkRequired();
    const { nc, objm } = await createConnection(flags);
    const bucket = flags.value<string>("bucket");
    const storage = flags.value<string>("storage");
    if (storage !== "file" && storage !== "memory") {
      throw new Error(`--storage must be file or memory`);
    }
    const runs = flags.value<number>("runs");
    const path = flags.value<string>("file");
    const { size } = await stat(path);

    // the bucket is deleted at the end, so never reuse an existing one
    const exists = await objm.open(bucket).then(() => true, () => false);
    if (exists) {
      console.log(`bucket ${bucket} exists, compare needs a new bucket (-b)`);
      await nc.close();
      return 1;
    }
    // one bucket with what the server supports; each handle picks its path
    const apiLvl = nc.info?.api_lvl ?? 0;
    await objm.create(bucket, {
      storage,
      compression: flags.value<boolean>("compression"),
      ...(apiLvl >= 4 ? { allowBatched: true } : {}),
    } as unknown as Partial<ObjectStoreOptions>);
    const modes = [];
    for (const mode of ["acked", "fast"]) {
      const os = await objm.open(
        bucket,
        { check: true, ...modeOpts(mode) } as { check: boolean },
      ).catch(() => null);
      if (os === null || putPath(os) === "acked" && mode !== "acked") {
        console.log(`${mode} not supported by the server, skipped`);
        continue;
      }
      modes.push({ label: putPath(os), os });
    }

    const avg: Record<string, number> = {};
    let ok = true;
    for (const { label, os } of modes) {
      let total = 0;
      for (let i = 0; i < runs; i++) {
        const name = `compare-${label.replace(" ", "-")}`;
        const start = performance.now();
        await os.put({ name }, readStream(path));
        const ms = performance.now() - start;
        total += ms;
        console.log(`${label} run ${i + 1}: ${rate(size, ms)}`);
        const stored = await os.info(name);
        ok = stored !== null && await verify(path, stored) && ok;
      }
      avg[label] = total / runs;
    }
    console.log("");
    for (const { label } of modes) {
      console.log(
        `${label} avg: ${rate(size, avg[label])} ${
          (avg["acked"] / avg[label]).toFixed(2)
        }x`,
      );
    }
    await objm.open(bucket).then((os) => os.destroy());
    await nc.close();
    return ok ? 0 : 1;
  },
});
compare.addFlag({
  name: "file",
  type: "string",
  short: "f",
  usage: "file to put",
  required: true,
});
compare.addFlag({
  name: "runs",
  type: "number",
  usage: "puts per mode",
  default: 3,
});
compare.addFlag({
  name: "storage",
  type: "string",
  usage: "bucket storage: file or memory",
  default: "file",
});
compare.addFlag({
  name: "compression",
  type: "boolean",
  usage: "create the bucket with s2 compression",
});

const pubcompare = root.addCommand({
  use: "pubcompare --file filepath [--runs 3] [--chunk 128k]",
  short:
    "publish a file as chunks to a temporary stream: sequential acks, atomic batch, fast ingest",
  run: async (_cmd, _args, flags) => {
    flags.checkRequired();
    const { nc } = await createConnection(flags);
    const runs = flags.value<number>("runs");
    const chunk = parseSize(flags.value<string>("chunk"));
    const batchSize = flags.value<number>("batch-size");
    const storage = flags.value<string>("storage");
    if (storage !== "file" && storage !== "memory") {
      throw new Error(`--storage must be file or memory`);
    }

    // preload so disk reads aren't measured
    const data = await readFile(flags.value<string>("file"));
    const chunks: Uint8Array[] = [];
    for (let i = 0; i < data.length; i += chunk) {
      chunks.push(data.subarray(i, i + chunk));
    }
    console.log(
      `${chunks.length} chunks of ${chunk} bytes, atomic batches of ${batchSize}`,
    );

    const apiLvl = nc.info?.api_lvl ?? 0;
    const jsm = await jetstreamManager(nc);
    const js = jetstream(nc);
    const name = `PUBCOMPARE_${nuid.next()}`;
    await jsm.streams.add(
      {
        name,
        subjects: [`${name}.>`],
        storage: storage as "file" | "memory",
        allow_atomic: apiLvl >= 2 ? true : undefined,
        allow_batched: apiLvl >= 4 ? true : undefined,
      } as Parameters<typeof jsm.streams.add>[0],
    );

    const modes: { label: string; fn: (subj: string) => Promise<void> }[] = [{
      label: "sequential",
      fn: async (subj) => {
        for (const c of chunks) {
          await js.publish(subj, c);
        }
      },
    }];
    if (apiLvl >= 2) {
      modes.push({
        label: "atomic batch",
        fn: async (subj) => {
          // the server caps a batch at 1000 messages, so large files take several
          for (let i = 0; i < chunks.length; i += batchSize) {
            const part = chunks.slice(i, i + batchSize);
            if (part.length === 1) {
              await js.publish(subj, part[0]);
              continue;
            }
            const b = await js.startBatch(subj, part[0]);
            for (let j = 1; j < part.length - 1; j++) {
              b.add(subj, part[j]);
            }
            await b.commit(subj, part[part.length - 1]);
          }
        },
      });
    } else {
      console.log("atomic batch needs 2.12+, skipped");
    }
    if (apiLvl >= 4) {
      modes.push({
        label: "fast ingest",
        fn: async (subj) => {
          const fi = await startFastIngest(nc, subj, chunks[0], {
            allowGaps: false,
          });
          for (let i = 1; i < chunks.length - 1; i++) {
            await fi.add(subj, chunks[i]);
          }
          await fi.last(subj, chunks[chunks.length - 1]);
        },
      });
    } else {
      console.log("fast ingest needs 2.14+, skipped");
    }

    const avg: Record<string, number> = {};
    let ok = true;
    try {
      for (const { label, fn } of modes) {
        let total = 0;
        for (let i = 0; i < runs; i++) {
          const subj = `${name}.${label.replace(" ", "_")}.${i}`;
          const start = performance.now();
          await fn(subj);
          const ms = performance.now() - start;
          total += ms;
          // every chunk must be stored
          const si = await jsm.streams.info(name, { subjects_filter: subj });
          const stored = si.state.subjects?.[subj] ?? 0;
          const good = stored === chunks.length;
          ok = ok && good;
          console.log(
            `${label} run ${i + 1}: ${rate(data.length, ms)} ${
              good
                ? `stored ${stored}`
                : `MISMATCH stored ${stored} of ${chunks.length}`
            }`,
          );
        }
        avg[label] = total / runs;
      }
    } finally {
      await jsm.streams.delete(name);
    }

    console.log("");
    const base = avg["sequential"];
    for (const { label } of modes) {
      console.log(
        `${label} avg: ${rate(data.length, avg[label])} ${
          (base / avg[label]).toFixed(2)
        }x`,
      );
    }
    await nc.close();
    return ok ? 0 : 1;
  },
});
pubcompare.addFlag({
  name: "file",
  type: "string",
  short: "f",
  usage: "file to publish",
  required: true,
});
pubcompare.addFlag({
  name: "runs",
  type: "number",
  usage: "runs per mode",
  default: 3,
});
pubcompare.addFlag({
  name: "chunk",
  type: "string",
  usage: "chunk size (k, m)",
  default: "128k",
});
pubcompare.addFlag({
  name: "batch-size",
  type: "number",
  usage: "messages per atomic batch (server max 1000)",
  default: 1000,
});
pubcompare.addFlag({
  name: "storage",
  type: "string",
  usage: "stream storage: file or memory",
  default: "file",
});

const get = root.addCommand({
  use: "get --name objname --file outfilepath",
  short: "get a file from the object store",
  run: async (_cmd, _args, flags) => {
    flags.checkRequired();
    const { nc, objm } = await createConnection(flags);
    const os = await objm.open(flags.value<string>("bucket"));
    const name = flags.value<string>("name");
    const start = performance.now();
    const d = await os.get(name);
    if (d === null) {
      console.log(`${name} not found`);
      await nc.close();
      return 1;
    }
    await d.data.pipeTo(writeStream(flags.value<string>("file")));
    const err = await d.error;
    const ms = performance.now() - start;
    if (err) {
      console.log(err);
      await nc.close();
      return 1;
    }
    console.log(`get ${name}: ${rate(d.info.size, ms)}`);
    const ok = await verify(flags.value<string>("file"), d.info);
    await nc.close();
    return ok ? 0 : 1;
  },
});

const rm = root.addCommand({
  use: "rm",
  short: "delete the bucket",
  run: async (_cmd, _args, flags) => {
    const { nc, objm } = await createConnection(flags);
    const bucket = flags.value<string>("bucket");
    const os = await objm.open(bucket);
    await os.destroy();
    console.log(`deleted ${bucket}`);
    await nc.close();
    return 0;
  },
});

for (const c of [put, get, rm, compare, pubcompare]) {
  c.addFlag({
    name: "server",
    type: "string",
    short: "s",
    usage: "nats server",
    default: "127.0.0.1:4222",
  });
  c.addFlag({
    name: "bucket",
    type: "string",
    short: "b",
    usage: "bucket name",
    default: "test",
  });
  c.addFlag({
    name: "sha",
    type: "string",
    usage: "sha256 backend: auto (the library's default), js or native",
    default: "auto",
  });
}

put.addFlag({
  name: "file",
  type: "string",
  short: "f",
  usage: "file to put",
  required: true,
});
put.addFlag({
  name: "name",
  type: "string",
  short: "n",
  usage: "object name (defaults to the file path)",
});
put.addFlag({
  name: "mode",
  type: "string",
  usage:
    "acked: per-chunk acks, fast: fast ingest, creates with allow_batched (2.14+)",
  default: "acked",
});
put.addFlag({
  name: "storage",
  type: "string",
  usage: "bucket storage: file or memory",
  default: "file",
});
put.addFlag({
  name: "compression",
  type: "boolean",
  usage: "create the bucket with s2 compression",
});

get.addFlag({
  name: "name",
  type: "string",
  short: "n",
  usage: "name of the object",
  required: true,
});
get.addFlag({
  name: "file",
  type: "string",
  short: "f",
  usage: "file to write",
  required: true,
});

const gen = root.addCommand({
  use: "generate --size 1m --file outfile",
  short: "generate random data of the specified size",
  run: async (cmd, _args, flags) => {
    flags.checkRequired();
    const size = parseSize(flags.value<string>("size"));

    const w = writeStream(flags.value<string>("file")).getWriter();
    for (let i = 0; i < size;) {
      const d = crypto.getRandomValues(
        new Uint8Array(Math.min(65536, size - i)),
      );
      await w.write(d);
      i += d.length;
    }
    await w.close();
    cmd.stdout(`generated ${size} bytes at ${flags.value("file")}\n`);
    return 0;
  },
});
gen.addFlag({
  name: "size",
  type: "string",
  usage: "size of the data to generate (k, m, g)",
  required: true,
});
gen.addFlag({
  name: "file",
  type: "string",
  short: "f",
  usage: "file to write",
  required: true,
});

process.exit(await root.execute());
