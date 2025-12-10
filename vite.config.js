import { fileURLToPath, URL } from "node:url";
import { defineConfig } from "vite";
export default defineConfig({
    server: {
        port: 5173,
        open: true,
    },
    resolve: {
        alias: {
            "@scrt": fileURLToPath(new URL("./src", import.meta.url)),
        },
    },
});
