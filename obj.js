import { connect } from "@nats-io/transport-node";
import { Objm } from "@nats-io/obj";
import { cli } from "@aricart/cobra";
import { Base64Codec, Base64UrlCodec, Base64UrlPaddedCodec } from "@nats-io/obj/internal";

const root = cli({
  use: "object store test",
});

async function createConnection() {
  const nc = await connect();
  const objm = new Objm(nc);
  return [nc, objm];
}

const file = {
  name: "file",
  type: "string",
  usage: "file path input/output",
  required: true,
};

const put = root.addCommand({
  name: "put",
  use: "put a file into the object store",
  run: async (cmd, args, flags) => {
    const [nc, objm] = await createConnection();
    console.log(await objm.list().next());
    const os = await objm.open("test")
      .catch((err) => {
        return objm.create("test");
      });

    const file = await Deno.open(flags.value("file"), { read: true });
    const oi = await os.put({ name: flags.value("file") }, file.readable);
    console.log(oi);

    await nc.close();
  },
});
put.addFlag(file);

const get = root.addCommand({
  name: "get",
  use: "get a file from the object store",
  run: async (cmd, args, flags) => {
    const [nc, objm] = await createConnection();
    const os = await objm.open("test");
    const d = await os.get(flags.value("name"));
    if (d) {
      console.log(d.info);
      const file = await Deno.open(flags.value("file"), {
        write: true,
        create: true,
      });
      const reader = d.data.getReader();
      const writer = file.writable.getWriter();
      while (true) {
        const { done, value } = await reader.read();
        if (done) {
          writer.close();
          break;
        }
        if (value) {
          await writer.write(value);
        }
      }
    }
    const err = await d?.error;
    if (err) {
      console.log(err);
    }
    console.log("done");

    await nc.close();
  },
});
get.addFlag({
  name: "name",
  type: "string",
  usage: "object name to get",
  required: true,
});
get.addFlag(file);

const gen = root.addCommand({
  name: "generate",
  use: "generate random data of the specified size",
  run: async (cmd, args, flags) => {
    flags.checkRequired();
    const s = flags.value("size");
    const re = /^(\d+)(\D+)?$/;
    const a = re.exec(s);
    if (a === null) {
      throw new Error("invalid size");
    }
    const n = parseInt(a[1]);
    let size = n;
    switch (a?.[2]?.toLowerCase()) {
      case "k":
      case "kib":
        size *= 1024;
        break;
      case "m":
      case "mib":
        size *= 1024 * 1024;
        break;
      case "g":
      case "gib":
        size *= 1024 * 1024 * 1024;
        break;
    }

    let d = new Uint8Array(0);
    for (let i = 0; i < size; i += d.length) {
      d = size > 65536 ? new Uint8Array(65536) : new Uint8Array(size);
      d = crypto.getRandomValues(d);
      await Deno.writeFile(
        flags.value("file"),
        d,
        i === 0 ? { create: true } : { append: true },
      );
    }
    cmd.stdout(`generated ${size} bytes at ${flags.value("file")}`);
    return Promise.resolve(0);
  },
});
gen.addFlag({
  name: "size",
  type: "string",
  usage: "size of the data to generate",
  required: true,
});

gen.addFlag(file);

const hash = root.addCommand({
  name: "hash",
  type: "string",
  use: "hash a file",
  run: async (cmd, args, flags) => {
    flags.checkRequired();
    const oldSha = new SHA256();
    const otherSha = sha256.create();

    const file = await Deno.open(flags.value("file"), { read: true });
    const reader = file.readable.getReader();
    while (true) {
      const { done, value } = await reader.read();
      if (done) {
        break;
      }
      if (value) {
        oldSha.update(value);
        otherSha.update(value);
      }
    }

    const oldDigest = await oldSha.digest();
    const newDigest = await otherSha.digest();

    const format = function(a, hex) {
      return {
        b64: Base64Codec.encode(a),
        b64url: Base64UrlCodec.encode(a),
        b64padded: Base64UrlPaddedCodec.encode(a),
        hex
      }
    }

    console.log("old", format(oldDigest, oldSha.digest("hex")));
    console.log("new", format(newDigest, otherSha.hex()));
  },
});
hash.addFlag(file);

await root.execute();
