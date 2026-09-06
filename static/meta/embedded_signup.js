/* PHANTA Meta Embedded Signup v4 launch client. Secrets never enter JS.
 *
 * FIXES IN THIS REVISION
 * ----------------------
 * 1. CSRF. phanta_app.py wraps the whole app in CSRFProtect and exempts only
 *    webhooks_bp and paystack_bp. The meta blueprint is NOT exempt, so the
 *    POSTs to /embedded-signup/start and /embedded-signup/callback were being
 *    rejected with 400 "The CSRF token is missing" before they ever reached
 *    the route. base.html already publishes <meta name="csrf-token">, so the
 *    token is now sent as X-CSRFToken on every POST.
 * 2. "Not configured" is reported as such. require_configured() answers 503
 *    with {message, missing: [...]}. That body was being thrown away and the
 *    user saw a generic failure, which reads as "PHANTA is broken" rather
 *    than "this deployment is missing META_APP_ID".
 * 3. Errors are surfaced instead of dying silently. Any throw inside the
 *    FB.login callback used to be swallowed by the SDK; it now rejects the
 *    promise so the status line actually says what went wrong.
 */
(function () {
  const button = document.getElementById("connect-whatsapp");
  const status = document.getElementById("status");
  if (!button) return;

  const csrfToken = (function () {
    const tag = document.querySelector('meta[name="csrf-token"]');
    return tag ? tag.getAttribute("content") : "";
  })();

  // WhatsApp Embedded Signup reports the selected business assets through a
  // browser message event. Keep the latest completion payload and send it to
  // the server with the authorization code. sessionInfo remains only as a
  // backwards-compatible fallback.
  let embeddedSignupAssets = {};
  window.addEventListener("message", function (event) {
    if (event.origin !== "https://www.facebook.com" && event.origin !== "https://web.facebook.com") return;
    let payload = event.data;
    if (typeof payload === "string") {
      try { payload = JSON.parse(payload); } catch (_) { return; }
    }
    if (!payload || payload.type !== "WA_EMBEDDED_SIGNUP") return;
    const eventName = payload.event || (payload.data && payload.data.event);
    const data = (payload.data && payload.data.data) || payload.data || {};
    if (eventName === "FINISH" || eventName === "FINISH_ONLY_WABA") {
      embeddedSignupAssets = {
        business_id: data.business_id || data.businessId || payload.business_id || payload.businessId,
        waba_id: data.waba_id || data.wabaId || payload.waba_id || payload.wabaId,
        phone_number_id: data.phone_number_id || data.phoneNumberId || payload.phone_number_id || payload.phoneNumberId
      };
    }
  });

  function setStatus(text, kind) {
    if (!status) return;
    status.textContent = text || "";
    status.classList.remove("is-error", "is-success");
    if (kind) status.classList.add(kind);
  }

  /* Turn a non-OK response into an Error carrying the most useful message the
     server gave us, rather than a generic one. */
  async function errorFrom(response, fallback) {
    const data = await response.json().catch(() => ({}));

    if (Array.isArray(data.missing) && data.missing.length) {
      return new Error(
        (data.message || "This integration is not configured on this deployment.") +
        " Missing: " + data.missing.join(", ")
      );
    }
    if (response.status === 401 || response.status === 403) {
      return new Error(
        data.error || data.message ||
        "You do not have permission to connect WhatsApp, or your session expired. Sign in again as an owner or admin."
      );
    }
    return new Error(data.error || data.message || fallback);
  }

  async function getConfig() {
    const response = await fetch("/integrations/meta/embedded-signup/config", {
      credentials: "same-origin",
      headers: { "Accept": "application/json" }
    });
    if (!response.ok) throw await errorFrom(response, "Unable to load Meta configuration.");

    const config = await response.json();
    if (!config.app_id || !config.config_id) {
      throw new Error(
        "Meta is not fully configured on this deployment. META_APP_ID and " +
        "META_WHATSAPP_CONFIG_ID must both be set."
      );
    }
    return config;
  }

  async function startSignup() {
    const response = await fetch("/integrations/meta/embedded-signup/start", {
      method: "POST",
      credentials: "same-origin",
      headers: {
        "Accept": "application/json",
        "X-CSRFToken": csrfToken
      }
    });
    if (!response.ok) throw await errorFrom(response, "Could not start WhatsApp connection.");
    return response.json();
  }

  function loadFacebookSdk(appId, version) {
    return new Promise((resolve, reject) => {
      const init = () => {
        if (!window.FB) {
          reject(new Error("Meta's Facebook SDK did not load. Check for an ad blocker or content blocker."));
          return;
        }
        window.FB.init({ appId: appId, cookie: true, xfbml: false, version: version });
        resolve(window.FB);
      };

      if (window.FB) { init(); return; }

      const existing = document.getElementById("facebook-jssdk");
      if (existing) {
        existing.addEventListener("load", init, { once: true });
        existing.addEventListener("error", function () {
          reject(new Error("Unable to load Meta's Facebook SDK."));
        }, { once: true });
        return;
      }

      const script = document.createElement("script");
      script.id = "facebook-jssdk";
      script.async = true;
      script.defer = true;
      script.crossOrigin = "anonymous";
      script.src = "https://connect.facebook.net/en_US/sdk.js";
      script.onload = init;
      script.onerror = function () {
        reject(new Error("Unable to load Meta's Facebook SDK."));
      };
      document.head.appendChild(script);
    });
  }

  async function completeSignup(start) {
    const config = await getConfig();
    const FB = await loadFacebookSdk(config.app_id, config.graph_api_version);

    return new Promise((resolve, reject) => {
      FB.login(function (response) {
        // Everything inside this callback runs under the SDK, which swallows
        // exceptions. Route all failures through reject() instead of throwing.
        (async function () {
          const auth = response && response.authResponse;
          if (!auth || !auth.code) {
            throw new Error("WhatsApp connection was cancelled or not completed.");
          }

          const sessionInfo = response.sessionInfo || {};
          const callbackResponse = await fetch("/integrations/meta/embedded-signup/callback", {
            method: "POST",
            credentials: "same-origin",
            headers: {
              "Content-Type": "application/json",
              "Accept": "application/json",
              "X-CSRFToken": csrfToken
            },
            body: JSON.stringify({
              code: auth.code,
              state_nonce: start.state_nonce,
              business_id: embeddedSignupAssets.business_id || sessionInfo.business_id || sessionInfo.businessId,
              waba_id: embeddedSignupAssets.waba_id || sessionInfo.waba_id || sessionInfo.wabaId,
              phone_number_id: embeddedSignupAssets.phone_number_id || sessionInfo.phone_number_id || sessionInfo.phoneNumberId
            })
          });

          if (!callbackResponse.ok) {
            throw await errorFrom(callbackResponse, "WhatsApp connection failed.");
          }
          return callbackResponse.json();
        })().then(resolve, reject);
      }, {
        config_id: config.config_id,
        response_type: "code",
        override_default_response_type: true,
        extras: { sessionInfoVersion: "3" }
      });
    });
  }

  button.addEventListener("click", async function () {
    button.disabled = true;
    setStatus("Opening Meta\u2026");

    try {
      const start = await startSignup();
      await completeSignup(start);
      setStatus("WhatsApp connected successfully.", "is-success");

      setTimeout(function () {
        if (window.location.pathname === "/onboarding/whatsapp") {
          window.location.href = "/onboarding/flyer-lady";
        } else {
          window.location.reload();
        }
      }, 900);
    } catch (error) {
      setStatus((error && error.message) || "WhatsApp connection failed.", "is-error");
      button.disabled = false;
    }
  });
})();
