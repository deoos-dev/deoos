const EMAIL_RE = /^[^\s@]{1,64}@[^\s@]{1,190}\.[^\s@]{2,24}$/;

function reply(request, status, body) {
  const wantsJson = (request.headers.get("Accept") || "").includes("application/json");
  if (wantsJson) {
    return new Response(JSON.stringify(body), {
      status,
      headers: { "Content-Type": "application/json" },
    });
  }
  if (status === 200) return Response.redirect(new URL("/?joined=1#su", request.url).toString(), 303);
  return new Response(body.error || "Error", { status, headers: { "Content-Type": "text/plain" } });
}

export async function onRequestPost({ request, env }) {
  let form;
  try {
    form = await request.formData();
  } catch {
    return reply(request, 400, { error: "Bad request." });
  }
  // Honeypot: bots fill hidden fields. Pretend success.
  if ((form.get("company") || "").toString().trim()) return reply(request, 200, { ok: true });

  const email = (form.get("email") || "").toString().trim().toLowerCase();
  if (!EMAIL_RE.test(email)) return reply(request, 400, { error: "That email doesn't look right." });

  const key = `signup:${email}`;
  const existing = await env.SIGNUPS.get(key);
  if (!existing) {
    await env.SIGNUPS.put(
      key,
      JSON.stringify({
        email,
        at: new Date().toISOString(),
        country: request.cf?.country || null,
        referer: request.headers.get("Referer") || null,
      })
    );
  }
  return reply(request, 200, { ok: true });
}

export function onRequestGet() {
  return new Response("Method not allowed", { status: 405 });
}
