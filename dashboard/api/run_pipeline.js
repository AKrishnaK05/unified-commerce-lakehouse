const GITHUB_API = "https://api.github.com";
const WORKFLOW_PATH = "/repos/AKrishnaK05/unified-commerce-lakehouse/actions/workflows/demo-pipeline.yml";

function getQuery(req) {
  return new URL(req.url || "http://localhost/api/run-pipeline", "http://localhost").searchParams;
}

async function githubRequest(path, token, options = {}) {
  const response = await fetch(`${GITHUB_API}${path}`, {
    ...options,
    headers: {
      Accept: "application/vnd.github+json",
      Authorization: `Bearer ${token}`,
      "X-GitHub-Api-Version": "2026-03-10",
      ...options.headers,
    },
  });

  if (!response.ok) {
    const errorText = await response.text();
    throw new Error(`GitHub API request failed (${response.status}): ${errorText}`);
  }

  return response.status === 204 ? null : response.json();
}

async function findLatestRun(token, startedAfter) {
  const data = await githubRequest(`${WORKFLOW_PATH}/runs?branch=main&event=workflow_dispatch&per_page=10`, token);
  return data.workflow_runs?.find((run) => new Date(run.created_at) >= startedAfter) || null;
}

export default async function handler(req, res) {
  const token = globalThis.process?.env?.GITHUB_TOKEN;

  if (!token) {
    return res.status(500).json({
      error: "GitHub token is not configured",
    });
  }

  try {
    const query = getQuery(req);
    if (req.method === "GET") {
      const runId = query.get("run_id");
      if (!runId) {
        return res.status(400).json({ error: "run_id is required" });
      }

      const run = await githubRequest(`/repos/AKrishnaK05/unified-commerce-lakehouse/actions/runs/${runId}`, token);
      return res.status(200).json({
        runId: run.id,
        status: run.status,
        conclusion: run.conclusion,
        htmlUrl: run.html_url,
      });
    }

    if (req.method !== "POST") {
      return res.status(405).json({ error: "Method not allowed" });
    }

    const startedAt = new Date();
    await githubRequest(`${WORKFLOW_PATH}/dispatches`, token, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ ref: "main" }),
    });

    let run = null;
    for (let attempt = 0; attempt < 8 && !run; attempt += 1) {
      await new Promise((resolve) => setTimeout(resolve, 1000));
      run = await findLatestRun(token, startedAt);
    }

    return res.status(200).json({
      success: true,
      message: "Pipeline triggered successfully",
      runId: run?.id || null,
    });
  } catch (error) {
    return res.status(500).json({
      error: "Pipeline request failed",
      details: error.message,
    });
  }
}