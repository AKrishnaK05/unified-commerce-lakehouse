import http from "node:http";
import handler from "./run_pipeline.js";

const port = Number(globalThis.process?.env?.API_PORT || 3000);

const server = http.createServer((req, res) => {
  const pathname = new URL(req.url || "/", "http://localhost").pathname;
  if (pathname !== "/api/run-pipeline") {
    res.writeHead(404, { "Content-Type": "application/json" });
    res.end(JSON.stringify({ error: "Not found" }));
    return;
  }

  handler(req, {
    status(statusCode) {
      res.statusCode = statusCode;
      return this;
    },
    json(payload) {
      res.setHeader("Content-Type", "application/json");
      res.end(JSON.stringify(payload));
    },
  }).catch((error) => {
    res.statusCode = 500;
    res.setHeader("Content-Type", "application/json");
    res.end(JSON.stringify({ error: "API handler failed", details: error.message }));
  });
});

server.on("error", (error) => {
  if (error.code === "EADDRINUSE") {
    console.error(`Pipeline API is already running at http://127.0.0.1:${port}/api/run-pipeline`);
    console.error("Stop the existing process or use API_PORT to choose another port.");
    return;
  }

  console.error("Unable to start pipeline API:", error.message);
});

server.listen(port, "127.0.0.1", () => {
  console.log(`Pipeline API listening at http://127.0.0.1:${port}/api/run-pipeline`);
});
