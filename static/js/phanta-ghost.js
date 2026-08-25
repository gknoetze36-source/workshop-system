/*
 * PHANTA Ghost — frontend-first, data-aware system guide.
 *
 * The browser may only describe facts explicitly supplied in
 * window.PHANTA_GHOST_DATA. It never invents counts, statuses, clients,
 * health states, currencies, or records that the backend did not provide.
 * No provider credentials are sent to the browser.
 */
(function () {
  'use strict';

  const DATA = window.PHANTA_GHOST_DATA || {};
  const has = (obj, key) => obj && Object.prototype.hasOwnProperty.call(obj, key) && obj[key] !== null && obj[key] !== undefined && obj[key] !== '';
  const connectionStatus = () => has(DATA.connectionHealth, 'status') ? String(DATA.connectionHealth.status) : null;
  const subscriptionStatus = () => has(DATA.billingState, 'subscription_status') && DATA.billingState.subscription_status !== 'not_configured' ? String(DATA.billingState.subscription_status) : null;

  const WORKSHOP_KNOWLEDGE = [
    { keys:['dashboard','home','today','vehicles today','bookings today'], answer:()=>{
      const parts=[];
      if (has(DATA,'todaysBookings')) parts.push(`${DATA.todaysBookings} booking${DATA.todaysBookings===1?'':'s'} today`);
      if (has(DATA,'vehiclesWaiting')) parts.push(`${DATA.vehiclesWaiting} vehicle${DATA.vehiclesWaiting===1?'':'s'} currently waiting/in progress`);
      if (has(DATA,'overdueVehicles')) parts.push(`${DATA.overdueVehicles} overdue vehicle${DATA.overdueVehicles===1?'':'s'}`);
      const live = parts.length ? `The dashboard currently has ${parts.join(', ')}.` : 'No live dashboard counts were supplied to PHANTA Ghost on this page.';
      return `${live} PHANTA does not expose customer appointment time slots; bookings are for the date and morning arrival.`;
    }},
    { keys:['booking','bookings','appointment','time slot','timeslot'], answer:'PHANTA uses date + morning arrival for customer-facing bookings. Customers do not choose or receive exact appointment time slots. Booking confirmation is the customer Yes/No decision PHANTA records.' },
    { keys:['customer','customers'], answer:'Customers and their vehicles are treated as one operational relationship. Use Customers & Vehicles to open records that the current backend supplies.' },
    { keys:['vehicle','vehicles','car'], answer:'Vehicle records contain the vehicle information, owner information and service/booking information available through the existing PHANTA backend.' },
    { keys:['ready','collection','ready for collection'], answer:'When the backend reports a booking in the ready-for-collection state, the dashboard provides the existing WhatsApp notification action. PHANTA Ghost does not claim that a message was sent unless the action reports success.' },
    { keys:['work to be done','unfinished','still work','finished'], answer:'The dashboard records the existing work-to-be-done action in PHANTA’s audit flow. If work is still outstanding, the existing backend follow-up behaviour is used. This does not authorise repairs or spending.' },
    { keys:['whatsapp','messages','message','meta'], answer:()=>{
      const status=connectionStatus();
      if(status==='reconnect_required') return 'The backend currently reports that WhatsApp requires reconnection. Open Settings → WhatsApp to reconnect.';
      if(status==='not_connected') return 'The backend currently reports that WhatsApp is not connected. Open Settings → WhatsApp to connect it.';
      return status ? `The backend currently reports the WhatsApp connection status as ${status.replaceAll('_',' ')}.` : 'No WhatsApp connection status was supplied to PHANTA Ghost on this page.';
    }},
    { keys:['service advisor','service','maintenance','recommendation'], answer:'Service Advisor is PHANTA’s vehicle/service intelligence capability. Its deterministic service rules remain the source of truth for maintenance intervals; it is separate from pricing, repair authorisation and public publishing.' },
    { keys:['price','pricing','quote','repair approval','spending'], answer:'PHANTA does not determine workshop pricing or authorise repairs/spending. Those remain workshop decisions.' },
    { keys:['flyer lady','flyer','special','promotion','facebook','instagram'], answer:'Flyer Lady is PHANTA’s public-promotion/publishing capability. It is strictly separate from WhatsApp customer messaging and Service Advisor.' },
    { keys:['onboarding','setup','connect','getting started'], answer:'The intended onboarding experience is simple: create the workshop account, enter workshop details, configure essentials, connect WhatsApp through Meta, review setup and enter the dashboard. Technical credentials remain platform configuration.' },
    { keys:['billing','subscription','payment'], answer:()=>{
      const status=subscriptionStatus();
      return status ? `The backend currently reports the subscription state as ${status.replaceAll('_',' ')}. PHANTA billing is separate from workshop repair pricing.` : 'No active subscription status was supplied to PHANTA Ghost on this page.';
    }},
    { keys:['login','password','username','email'], answer:'PHANTA authenticates the username/email against the user account. For workshop users, the email is intended to be the practical username.' },
    { keys:['error','broken','not working','problem','help','issue','troubleshoot'], answer:'Tell me what you are trying to do and what you see on screen. I can explain the PHANTA workflow and only use live facts that this page has actually supplied.' }
  ];

  const ADMIN_KNOWLEDGE = [
    { keys:['dashboard','home','platform','control center'], answer:()=>{
      const parts=[];
      if(has(DATA,'metaConnections')) parts.push(`${DATA.metaConnections} Meta connection records`);
      if(has(DATA.aiUsage,'requests')) parts.push(`${DATA.aiUsage.requests} AI requests`);
      if(has(DATA,'integrationErrorsCount')) parts.push(`${DATA.integrationErrorsCount} integration errors returned by the audit query`);
      return parts.length ? `This is the PHANTA Platform Control Center. The page currently has ${parts.join(', ')}.` : 'This is the PHANTA Platform Control Center. No live platform data is currently supplied to this page.';
    }},
    { keys:['client','clients','workshop','workshops','tenant','tenants'], answer:'Per-workshop client records are not supplied to this page, so PHANTA Ghost will not invent client names, statuses or health information.' },
    { keys:['audit','integration','audit center','integration center'], answer:'The current platform page can show the aggregate integration data and integration-error records actually supplied by the backend. Per-tenant drill-down is not displayed until a real backend read endpoint supplies it.' },
    { keys:['meta','whatsapp','connection','connections'], answer:()=>{
      const h=DATA.connectionHealth||{};
      const entries=Object.entries(h);
      return entries.length ? `The backend currently reports these aggregate Meta/WhatsApp connection states: ${entries.map(([k,v])=>`${k.replaceAll('_',' ')}: ${v}`).join(', ')}.` : 'No Meta/WhatsApp connection data is currently supplied to this page.';
    }},
    { keys:['billing','subscription','subscriptions','payment'], answer:()=>{
      const b=DATA.billingState||{};
      const entries=Object.entries(b);
      return entries.length ? `The backend currently reports these aggregate billing states: ${entries.map(([k,v])=>`${k.replaceAll('_',' ')}: ${v}`).join(', ')}.` : 'No billing data is currently supplied to this page.';
    }},
    { keys:['ai','artificial intelligence','usage','cost','tokens'], answer:()=>{
      const parts=[];
      if(has(DATA.aiUsage,'requests')) parts.push(`${DATA.aiUsage.requests} requests`);
      if(has(DATA.aiUsage,'input_tokens')) parts.push(`${DATA.aiUsage.input_tokens} input tokens`);
      if(has(DATA.aiUsage,'output_tokens')) parts.push(`${DATA.aiUsage.output_tokens} output tokens`);
      return parts.length ? `The backend currently reports ${parts.join(', ')}.` : 'No AI usage data is currently supplied to this page.';
    }},
    { keys:['error','errors','integration error','webhook','failure','failed'], answer:()=>{
      if(!has(DATA,'integrationErrorsCount')) return 'No integration-error count was supplied to PHANTA Ghost on this page.';
      return DATA.integrationErrorsCount ? `The backend returned ${DATA.integrationErrorsCount} integration error${DATA.integrationErrorsCount===1?'':'s'} in the current audit query. The page shows only fields actually supplied by that query.` : 'The current backend audit query returned zero integration errors.';
    }},
    { keys:['flyer lady','flyer','promotion','publishing'], answer:'Flyer Lady is the public-promotion/publishing capability. It must remain separate from WhatsApp Messages and Service Advisor.' },
    { keys:['service advisor','service','maintenance'], answer:'Service Advisor is the vehicle/service intelligence capability. Its deterministic maintenance rules remain distinct from pricing, repair authorisation and public publishing.' },
    { keys:['separate','separation','architecture','boundary','boundaries'], answer:'PHANTA has three strictly separate capabilities: WhatsApp Messages/communication, Service Advisor, and Flyer Lady. Flyer Lady is public promotion/publishing and must not alter, replace or interfere with WhatsApp messaging or Service Advisor.' },
    { keys:['workshop dashboard','workshop user','reception'], answer:'Workshop users get the operational workshop dashboard. Platform diagnostics are intentionally kept out of their experience.' },
    { keys:['onboarding','meta onboarding','signup'], answer:'The goal is simple workshop onboarding. Meta technical configuration belongs on the platform side; the workshop should experience as few technical steps as possible.' },
    { keys:['what can you see','what do you know','system information'], answer:'I can explain the PHANTA architecture and the live information explicitly supplied to this page. I cannot truthfully claim to know backend data that this page has not been given.' },
    { keys:['refresh','reload','live','status'], answer:'Use Refresh status in the Platform Control Center. It calls the existing platform dashboard data endpoint and refreshes the values supplied by that endpoint.' },
    { keys:['help','support','problem','broken','not working'], answer:'Tell me the exact symptom. I can map it to the PHANTA component involved using only the information and architecture available to this page.' }
  ];

  function normalize(text){ return String(text||'').toLowerCase().replace(/[^a-z0-9\s?]/g,' ').replace(/\s+/g,' ').trim(); }
  async function findAnswer(query){
    const q=normalize(query);
    if(!q) return DATA.admin ? 'Ask me about the platform information PHANTA has actually supplied.' : 'Ask me about the workshop information PHANTA has actually supplied.';
    try {
      // Same CSRF gap as the dashboard's postJson() -- confirmed the exact
      // same way: /api/ghost/ask is not CSRF-exempt, and this fetch never
      // sent a token. Every real question asked through this widget would
      // have failed with a CSRF error before ever reaching routes/ghost.py.
      const csrfToken = document.querySelector('meta[name="csrf-token"]')?.content || '';
      const response = await fetch('/api/ghost/ask', {
        method:'POST',
        headers:{'Content-Type':'application/json','Accept':'application/json','X-CSRFToken':csrfToken},
        credentials:'same-origin',
        body:JSON.stringify({question:String(query)})
      });
      const result = await response.json().catch(()=>({}));
      if(!response.ok) throw new Error(result.error || 'PHANTA Ghost could not reach its information service.');
      return result.answer || 'PHANTA Ghost received no answer from the backend information service.';
    } catch (error) {
      return `I could not access the live PHANTA information service right now. ${error.message}`;
    }
  }
  function addMessage(container,role,text){const el=document.createElement('div');el.className=`ghost-message ${role}`;el.textContent=text;container.appendChild(el);container.scrollTop=container.scrollHeight;}
  function addTyping(container){const el=document.createElement('div');el.className='ghost-message bot';el.dataset.typing='true';el.innerHTML='<span class="ghost-typing"><i></i><i></i><i></i></span>';container.appendChild(el);container.scrollTop=container.scrollHeight;return el;}

  function init(){
    if(!DATA || (!DATA.admin && !document.querySelector('.phanta-workshop-shell'))) return;
    const launcher=document.createElement('button'); launcher.type='button'; launcher.className='phanta-ghost-launcher'; launcher.setAttribute('aria-label','Open PHANTA Ghost assistant'); launcher.innerHTML='<span class="ghost-shape" aria-hidden="true"></span><span class="ghost-bubble">Need help? I only use information PHANTA supplies to this page.</span>';
    const panel=document.createElement('section'); panel.className='phanta-ghost-panel'; panel.setAttribute('aria-label','PHANTA Ghost assistant');
    const mode=DATA.admin?'Platform operator guide':'Your workshop guide';
    const suggestions=DATA.admin?['Integration errors','AI usage','PHANTA boundaries']:['Today','Service Advisor','WhatsApp','Flyer Lady'];
    panel.innerHTML=`<header class="phanta-ghost-header"><div class="ghost-title"><span class="ghost-shape mini-ghost" aria-hidden="true"></span><div><strong>PHANTA Ghost</strong><span>${mode}</span></div></div><button type="button" class="button ghost-close" aria-label="Close assistant">×</button></header><div class="phanta-ghost-messages" aria-live="polite"></div><div><div class="phanta-ghost-suggestions">${suggestions.map((s)=>`<button type="button" class="button ghost-suggestion" data-ghost-prompt="${s}">${s}</button>`).join('')}</div><form class="phanta-ghost-input"><input type="text" autocomplete="off" placeholder="Ask PHANTA Ghost..." aria-label="Ask PHANTA Ghost"><button type="submit" class="button primary">Ask</button></form></div>`;
    document.body.appendChild(panel); document.body.appendChild(launcher);
    const messages=panel.querySelector('.phanta-ghost-messages'),input=panel.querySelector('input');
    addMessage(messages,'bot',DATA.admin?'Hi 👻 I’m PHANTA Ghost. I can explain PHANTA and the live information explicitly supplied to this platform page.':'Hi 👻 I’m PHANTA Ghost. I can explain PHANTA, guide you through the workshop workflow, and use only information explicitly supplied to this page.');
    async function ask(text){const query=String(text||'').trim();if(!query)return;addMessage(messages,'user',query);input.value='';const typing=addTyping(messages);const answer=await findAnswer(query);typing.remove();addMessage(messages,'bot',answer);}
    launcher.addEventListener('click',()=>{const open=panel.classList.toggle('is-open');launcher.setAttribute('aria-expanded',open?'true':'false');if(open)window.setTimeout(()=>input.focus(),50);});
    panel.querySelector('.ghost-close').addEventListener('click',()=>panel.classList.remove('is-open'));
    panel.querySelector('form').addEventListener('submit',e=>{e.preventDefault();ask(input.value);});
    panel.querySelectorAll('[data-ghost-prompt]').forEach(btn=>btn.addEventListener('click',()=>ask(btn.dataset.ghostPrompt)));
    document.addEventListener('keydown',e=>{if(e.key==='Escape')panel.classList.remove('is-open');});
  }
  if(document.readyState==='loading')document.addEventListener('DOMContentLoaded',init);else init();
})();
