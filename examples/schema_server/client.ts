import { SchemaHttpClient } from "../../ts/apiClient";
import { createRowDecoder, parseSCRT } from "../../ts/scrt";

interface Message {
    MsgID: number;
    User: number;
    Text: string;
    Lang: string;
}

async function main() {
    const baseUrl = process.env.SCRT_SERVER ?? "http://localhost:8080";
    const client = new SchemaHttpClient(baseUrl);
    const schema = await client.schema("Message");

    const response = await fetch(new URL("/api/messages?format=text", baseUrl));
    if (!response.ok) {
        throw new Error(`message fetch failed: ${response.status} ${response.statusText}`);
    }
    const text = await response.text();
    const doc = parseSCRT(text, `${baseUrl}/api/messages`);
    const rows = doc.records(schema.name);
    const decode = createRowDecoder(schema, () => ({
        MsgID: 0,
        User: 0,
        Text: "",
        Lang: "",
    }));
    const messages = rows.map((row) => decode(row));

    console.log(`Decoded ${messages.length} messages`);
    for (const msg of messages) {
        console.log(`#${msg.MsgID} [user=${msg.User}] (${msg.Lang}) ${msg.Text}`);
    }
}

main().catch((err) => {
    console.error(err);
    process.exitCode = 1;
});
