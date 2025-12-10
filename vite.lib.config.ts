import { defineConfig } from "vite";
import { fileURLToPath, URL } from "node:url";

export default defineConfig({
    build: {
        lib: {
            entry: fileURLToPath(new URL("./src/index.ts", import.meta.url)),
            name: "scrt",
            fileName: (format) => `scrt.${format}.js`,
            formats: ["es", "cjs"],
        },
        rollupOptions: {
            external: [],
        },
        sourcemap: true,
    },
});
