import { SchemaHttpClient } from "../../ts/apiClient";

interface Message {
    MsgID: number;
    User: number;
    Text: string;
    Lang: string;
}

async function main() {
    const baseUrl = process.env.SCRT_SERVER ?? "http://localhost:8080";
    const documentName = process.env.SCRT_DOCUMENT ?? "message";
    const client = new SchemaHttpClient({ baseUrl, defaultDocument: documentName });
    const messages = await client.fetchRecords<Message>("Message", () => ({
        MsgID: 0,
        User: 0,
        Text: "",
        Lang: "",
    }), { document: documentName });

    console.log(`Decoded ${messages.length} messages from document "${documentName}"`);
    for (const msg of messages) {
        console.log(`#${msg.MsgID} [user=${msg.User}] (${msg.Lang}) ${msg.Text}`);
    }
}

main().catch((err) => {
    console.error(err);
    process.exitCode = 1;
});
