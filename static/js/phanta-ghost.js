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

  function normalize(text){ return String(text||'').toLowerCase().replace(/[^a-z0-9\s?]/g,' ').replace(/\s+/g,' ').trim(); }
  async function findAnswer(query){
    const q=normalize(query);
    if(!q) return {answer:'Type a question, or tap one of the suggestions below.'};
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
      return result.answer ? result : {answer:'Sorry, I didn\'t get an answer back. Please try again.'};
    } catch (error) {
      return {answer:`Sorry, I can't reach PHANTA right now. Check your internet connection and try again. (${error.message})`};
    }
  }
  function addMessage(container,role,text){const el=document.createElement('div');el.className=`ghost-message ${role}`;el.textContent=text;container.appendChild(el);container.scrollTop=container.scrollHeight;}
  // A helper-style reply: intro text, numbered steps, buttons to the right page, and follow-up suggestions.
  function addReply(container,reply,onAsk){
    const el=document.createElement('div');el.className='ghost-message bot';
    const intro=document.createElement('p');intro.className='ghost-intro';intro.textContent=reply.intro||reply.answer||'';el.appendChild(intro);
    if(reply.steps&&reply.steps.length){const ol=document.createElement('ol');ol.className='ghost-steps';reply.steps.forEach(t=>{const li=document.createElement('li');li.textContent=t;ol.appendChild(li);});el.appendChild(ol);}
    if(reply.links&&reply.links.length){const row=document.createElement('div');row.className='ghost-links';reply.links.forEach(l=>{const a=document.createElement('a');a.className='button secondary ghost-link-button';a.href=l.url;a.textContent=l.label+' →';row.appendChild(a);});el.appendChild(row);}
    container.appendChild(el);
    if(reply.suggestions&&reply.suggestions.length){const row=document.createElement('div');row.className='ghost-followups';const lab=document.createElement('span');lab.textContent='You could also ask:';row.appendChild(lab);reply.suggestions.forEach(q=>{const b=document.createElement('button');b.type='button';b.className='button ghost-suggestion';b.textContent=q;b.addEventListener('click',()=>onAsk(q));row.appendChild(b);});container.appendChild(row);}
    container.scrollTop=container.scrollHeight;
  }
  function addTyping(container){const el=document.createElement('div');el.className='ghost-message bot';el.dataset.typing='true';el.innerHTML='<span class="ghost-typing"><i></i><i></i><i></i></span>';container.appendChild(el);container.scrollTop=container.scrollHeight;return el;}

  function init(){
    if(!DATA || (!DATA.admin && !document.querySelector('.phanta-workshop-shell'))) return;
    const launcher=document.createElement('button'); launcher.type='button'; launcher.className='phanta-ghost-launcher'; launcher.setAttribute('aria-label','Open PHANTA Ghost assistant'); launcher.innerHTML='<span class="ghost-shape" aria-hidden="true"></span><span class="ghost-bubble">Need a hand? Ask me anything about PHANTA.</span>';
    const panel=document.createElement('section'); panel.className='phanta-ghost-panel'; panel.setAttribute('aria-label','PHANTA Ghost assistant');
    const mode=DATA.admin?'Platform operator guide':'Your workshop guide';
    const suggestions=DATA.admin?['Integration errors','AI usage','PHANTA boundaries']:['What can you do?','Today','Add a booking','Connect WhatsApp','Results'];
    panel.innerHTML=`<header class="phanta-ghost-header"><div class="ghost-title"><span class="ghost-shape mini-ghost" aria-hidden="true"></span><div><strong>PHANTA Ghost</strong><span>${mode}</span></div></div><button type="button" class="button ghost-close" aria-label="Close assistant">×</button></header><div class="phanta-ghost-messages" aria-live="polite"></div><div><div class="phanta-ghost-suggestions">${suggestions.map((s)=>`<button type="button" class="button ghost-suggestion" data-ghost-prompt="${s}">${s}</button>`).join('')}</div><form class="phanta-ghost-input"><input type="text" autocomplete="off" placeholder="Ask PHANTA Ghost..." aria-label="Ask PHANTA Ghost"><button type="submit" class="button primary">Ask</button></form></div>`;
    document.body.appendChild(panel); document.body.appendChild(launcher);
    const messages=panel.querySelector('.phanta-ghost-messages'),input=panel.querySelector('input');
    if(!window.PHANTA_GHOST_WELCOME) addMessage(messages,'bot',DATA.admin?'Hi 👻 I’m PHANTA Ghost. I can explain PHANTA and the live information explicitly supplied to this platform page.':'Hi 👋 I’m the PHANTA Ghost, your workshop helper. Ask me how to do anything in PHANTA (bookings, WhatsApp, posting specials, ads, results) and I’ll walk you through it step by step.');
    // After onboarding: keep asking to be clicked until the user opens the Ghost once.
    const welcome=window.PHANTA_GHOST_WELCOME;
    if(welcome){
      launcher.classList.add('ghost-welcome');
      launcher.querySelector('.ghost-bubble').textContent='👋 Please click me!';
    }
    function showWelcome(){
      const el=document.createElement('div');el.className='ghost-message bot';
      el.append('Hi 👋 Welcome to PHANTA! I’m the Ghost, your workshop guide. Before you start, please read these two guides. They show exactly what to set up on your side:');
      const list=document.createElement('ol');
      [['Meta setup: WhatsApp, Facebook, Instagram and paid boosts',welcome.meta],['Google Ads setup: Search ads for your specials',welcome.google]].forEach(([label,href])=>{
        const li=document.createElement('li');const a=document.createElement('a');a.href=href;a.textContent=label;li.appendChild(a);list.appendChild(li);});
      el.appendChild(list);el.append('When you’re done, ask me anything.');
      messages.appendChild(el);messages.scrollTop=messages.scrollHeight;
      launcher.classList.remove('ghost-welcome');
      launcher.querySelector('.ghost-bubble').textContent='Need help? Ask PHANTA Ghost.';
      const csrfToken=document.querySelector('meta[name="csrf-token"]')?.content||'';
      fetch('/api/ghost/welcome-seen',{method:'POST',headers:{'X-CSRFToken':csrfToken},credentials:'same-origin'}).catch(()=>{});
    }
    let welcomePending=!!welcome;
    async function ask(text){const query=String(text||'').trim();if(!query)return;addMessage(messages,'user',query);input.value='';const typing=addTyping(messages);const reply=await findAnswer(query);typing.remove();addReply(messages,reply,ask);}
    launcher.addEventListener('click',()=>{if(welcomePending){welcomePending=false;showWelcome();}const open=panel.classList.toggle('is-open');launcher.setAttribute('aria-expanded',open?'true':'false');if(open)window.setTimeout(()=>input.focus(),50);});
    panel.querySelector('.ghost-close').addEventListener('click',()=>panel.classList.remove('is-open'));
    panel.querySelector('form').addEventListener('submit',e=>{e.preventDefault();ask(input.value);});
    panel.querySelectorAll('[data-ghost-prompt]').forEach(btn=>btn.addEventListener('click',()=>ask(btn.dataset.ghostPrompt)));
    document.addEventListener('keydown',e=>{if(e.key==='Escape')panel.classList.remove('is-open');});
  }
  if(document.readyState==='loading')document.addEventListener('DOMContentLoaded',init);else init();
})();
