const elements = Object.fromEntries([...document.querySelectorAll("[id]")].map((el) => [el.id, el]));
let queues = [];
let timer;

async function api(path, options = {}) {
  const response = await fetch(path, {
    ...options,
    headers: { "content-type": "application/json", ...options.headers },
  });
  const body = await response.json();
  if (!response.ok) throw new Error(body.error?.message ?? `HTTP ${response.status}`);
  return body;
}

function toast(message, error = false) {
  clearTimeout(timer);
  elements.toast.textContent = message;
  elements.toast.className = `show${error ? " error" : ""}`;
  timer = setTimeout(() => { elements.toast.className = ""; }, 3000);
}

function selectOptions() {
  const currentPublish = elements["publish-queue"].value;
  const currentConsume = elements["consume-queue"].value;
  const html = queues.map((queue) => `<option value="${queue.name}">${queue.name}</option>`).join("");
  elements["publish-queue"].innerHTML = html;
  elements["consume-queue"].innerHTML = html;
  if (queues.some((q) => q.name === currentPublish)) elements["publish-queue"].value = currentPublish;
  if (queues.some((q) => q.name === currentConsume)) elements["consume-queue"].value = currentConsume;
}

async function loadQueues() {
  const result = await api("/v1/queues");
  queues = result.queues;
  selectOptions();
  await refresh();
}

async function refresh() {
  const name = elements["consume-queue"].value;
  if (!name) {
    elements["queue-meta"].innerHTML = "";
    elements.messages.innerHTML = '<p class="empty">Create a queue to get started.</p>';
    return;
  }
  const [description, listing] = await Promise.all([
    api(`/v1/queues/${encodeURIComponent(name)}`),
    api(`/v1/queues/${encodeURIComponent(name)}/messages?limit=100`),
  ]);
  const queue = description.queue;
  const labels = [
    [queue.stats.available, "Available"],
    [queue.stats.delayed, "Delayed"],
    [queue.stats.inFlight, "In flight"],
    [`${queue.priority ? "Priority + " : ""}${queue.discipline.toUpperCase()}`, "Discipline"],
  ];
  elements["queue-meta"].innerHTML = labels.map(([value, label]) => `<div class="metric"><strong>${value}</strong><span>${label}</span></div>`).join("");
  if (listing.messages.length === 0) {
    elements.messages.innerHTML = '<p class="empty">No messages. This queue is resting.</p>';
    return;
  }
  elements.messages.innerHTML = listing.messages.map((message) => `
    <div class="message">
      <div>
        <code>${escapeHtml(JSON.stringify(message.payload))}</code>
        <small><span class="badge">${message.state}</span><span class="badge">p${message.priority}</span>${message.delayed ? '<span class="badge">delayed</span>' : ""} ${message.id.slice(0, 8)} · attempts ${message.attempts}</small>
      </div>
      ${message.receipt ? `<div class="actions"><button data-action="ack" data-id="${message.id}" data-receipt="${message.receipt}">Ack</button><button class="nack" data-action="nack" data-id="${message.id}" data-receipt="${message.receipt}">Retry</button></div>` : ""}
    </div>`).join("");
}

function escapeHtml(value) {
  return value.replace(/[&<>'"]/g, (character) => ({ "&": "&amp;", "<": "&lt;", ">": "&gt;", "'": "&#39;", '"': "&quot;" })[character]);
}

elements["create-form"].addEventListener("submit", async (event) => {
  event.preventDefault();
  try {
    await api("/v1/queues", { method: "POST", body: JSON.stringify({
      name: elements["queue-name"].value,
      discipline: elements.discipline.value,
      priority: elements["priority-enabled"].checked,
    }) });
    toast("Queue created. Time to feed it.");
    await loadQueues();
  } catch (error) { toast(error.message, true); }
});

elements["publish-form"].addEventListener("submit", async (event) => {
  event.preventDefault();
  try {
    const name = elements["publish-queue"].value;
    if (!name) throw new Error("Create a queue first");
    await api(`/v1/queues/${encodeURIComponent(name)}/messages`, { method: "POST", body: JSON.stringify({
      payload: JSON.parse(elements.payload.value),
      priority: Number(elements["message-priority"].value),
      delayMs: Number(elements.delay.value),
      ...(elements.dedupe.value ? { idempotencyKey: elements.dedupe.value } : {}),
    }) });
    toast("Message enqueued durably.");
    elements["consume-queue"].value = name;
    await refresh();
  } catch (error) { toast(error.message, true); }
});

elements.claim.addEventListener("click", async () => {
  try {
    const name = elements["consume-queue"].value;
    const result = await api(`/v1/queues/${encodeURIComponent(name)}/claims`, { method: "POST", body: JSON.stringify({ limit: 1 }) });
    toast(result.messages.length ? "Claimed one message." : "Nothing is available yet.");
    await refresh();
  } catch (error) { toast(error.message, true); }
});

elements.messages.addEventListener("click", async (event) => {
  const button = event.target.closest("button[data-action]");
  if (!button) return;
  try {
    const name = elements["consume-queue"].value;
    const action = button.dataset.action;
    await api(`/v1/queues/${encodeURIComponent(name)}/messages/${button.dataset.id}/${action}`, {
      method: "POST",
      body: JSON.stringify({ receipt: button.dataset.receipt, ...(action === "nack" ? { delayMs: 1000 } : {}) }),
    });
    toast(action === "ack" ? "Work acknowledged." : "Message scheduled for retry.");
    await refresh();
  } catch (error) { toast(error.message, true); }
});

elements.refresh.addEventListener("click", () => refresh().catch((error) => toast(error.message, true)));
elements["consume-queue"].addEventListener("change", () => refresh().catch((error) => toast(error.message, true)));

api("/healthz").then(() => {
  elements.health.textContent = "Engine online";
  elements.health.parentElement.classList.add("up");
  return loadQueues();
}).catch((error) => {
  elements.health.textContent = "Engine offline";
  toast(error.message, true);
});
