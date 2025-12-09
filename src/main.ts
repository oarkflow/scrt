import { SchemaHttpClient } from "../ts/apiClient";

type Message = {
    MsgID: number;
    User: number;
    Text: string;
    Lang: string;
};

async function loadMessages(baseUrl: string, target: HTMLElement, status: HTMLElement) {
    try {
        status.textContent = "Fetching schema bundle…";
        const client = new SchemaHttpClient(baseUrl);
        const records = await client.fetchRecords<Message>("Message", () => ({
            MsgID: 0,
            User: 0,
            Text: "",
            Lang: "",
        }));
        renderMessages(records, target);
        status.textContent = `Loaded ${records.length} record(s)`;
    } catch (err) {
        status.textContent = (err as Error).message;
        target.replaceChildren();
    }
}

function renderMessages(records: Message[], target: HTMLElement) {
    target.replaceChildren(
        ...records.map((record) => {
            const li = document.createElement("li");
            li.innerHTML = `<strong>#${record.MsgID}</strong> · user ${record.User} · ${record.Lang}<br>${record.Text}`;
            return li;
        }),
    );
}

function main() {
    const input = document.querySelector<HTMLInputElement>("#server");
    const button = document.querySelector<HTMLButtonElement>("#load");
    const list = document.querySelector<HTMLUListElement>("#messages");
    const status = document.querySelector<HTMLElement>("#status");
    if (!input || !button || !list || !status) {
        throw new Error("missing DOM nodes");
    }
    const defaultBase = import.meta.env.VITE_SCRT_SERVER ?? input.value;
    input.value = defaultBase;
    button.addEventListener("click", () => {
        loadMessages(input.value, list, status);
    });
}

main();
