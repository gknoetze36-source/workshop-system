/* PHANTA Meta Embedded Signup v4 launch client. Secrets never enter JS. */
(async function () {
  const button = document.getElementById("connect-whatsapp");
  const status = document.getElementById("status");
  if (!button) return;

  const setStatus = (text) => { if (status) status.textContent = text; };

  async function getConfig() {
    const response = await fetch("/integrations/meta/embedded-signup/config", {
      credentials: "same-origin"
    });
    if (!response.ok) throw new Error("Unable to load Meta configuration.");
    return response.json();
  }

  async function startSignup() {
    const response = await fetch("/integrations/meta/embedded-signup/start", {
      method: "POST",
      credentials: "same-origin"
    });
    const data = await response.json().catch(() => ({}));
    if (!response.ok) throw new Error(data.error || "Could not start WhatsApp connection.");
    return data;
  }

  function loadFacebookSdk(appId, version) {
    return new Promise((resolve, reject) => {
      if (window.FB) {
        window.FB.init({ appId, cookie: true, xfbml: false, version });
        resolve(window.FB);
        return;
      }

      const existing = document.getElementById("facebook-jssdk");
      const finish = () => {
        if (!window.FB) {
          reject(new Error("Meta's Facebook SDK did not load."));
          return;
        }
        window.FB.init({ appId, cookie: true, xfbml: false, version });
        resolve(window.FB);
      };

      if (existing) {
        existing.addEventListener("load", finish, { once: true });
        existing.addEventListener("error", () => reject(new Error("Unable to load Meta's Facebook SDK.")), { once: true });
        return;
      }

      const script = document.createElement("script");
      script.id = "facebook-jssdk";
      script.async = true;
      script.defer = true;
      script.src = "https://connect.facebook.net/en_US/sdk.js";
      script.onload = finish;
      script.onerror = () => reject(new Error("Unable to load Meta's Facebook SDK."));
      document.head.appendChild(script);
    });
  }

  async function completeSignup(start) {
    const config = await getConfig();
    const FB = await loadFacebookSdk(config.app_id, config.graph_api_version);

    return new Promise((resolve, reject) => {
      FB.login(async (response) => {
        const auth = response && response.authResponse;
        if (!auth || !auth.code) {
          reject(new Error("WhatsApp connection was cancelled or not completed."));
          return;
        }

        const sessionInfo = response.sessionInfo || {};
        const callbackResponse = await fetch("/integrations/meta/embedded-signup/callback", {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          credentials: "same-origin",
          body: JSON.stringify({
            code: auth.code,
            state_nonce: start.state_nonce,
            business_id: sessionInfo.business_id || sessionInfo.businessId,
            waba_id: sessionInfo.waba_id || sessionInfo.wabaId,
            phone_number_id: sessionInfo.phone_number_id || sessionInfo.phoneNumberId
          })
        });

        const data = await callbackResponse.json().catch(() => ({}));
        if (!callbackResponse.ok) {
          reject(new Error(data.error || "WhatsApp connection failed."));
          return;
        }
        resolve(data);
      }, {
        config_id: config.config_id,
        response_type: "code",
        override_default_response_type: true,
        extras: { sessionInfoVersion: "3" }
      });
    });
  }

  button.addEventListener("click", async () => {
    button.disabled = true;
    setStatus("Opening Meta…");

    try {
      const start = await startSignup();
      await completeSignup(start);
      setStatus("WhatsApp connected successfully.");
      setTimeout(() => {
        if (window.location.pathname === "/onboarding/whatsapp") {
          window.location.href = "/onboarding/automation";
        }
      }, 800);
    } catch (error) {
      setStatus(error.message || "WhatsApp connection failed.");
      button.disabled = false;
    }
  });
})();
