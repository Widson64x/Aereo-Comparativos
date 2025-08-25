// static/js/app.js
(function () {
  // ===== AOS
  if (window.AOS) AOS.init({ once: true, duration: 500, easing: 'ease-out-quart' });

  // ===== Tippy tooltips
  if (window.tippy) tippy('[data-tippy-content]', { theme: 'light', delay: [200, 0] });

  // ===== Theme toggle (localStorage)
  const html = document.documentElement;
  const saved = localStorage.getItem('aa_theme');
  if (saved) html.setAttribute('data-bs-theme', saved);
  const themeBtn = document.getElementById('theme-toggle');
  const syncIcons = () => {
    const isDark = html.getAttribute('data-bs-theme') === 'dark';
    const sun = themeBtn?.querySelector('.icon-sun');
    const moon = themeBtn?.querySelector('.icon-moon');
    if (sun && moon) { sun.classList.toggle('d-none', isDark); moon.classList.toggle('d-none', !isDark); }
  };
  syncIcons();
  themeBtn?.addEventListener('click', () => {
    const current = html.getAttribute('data-bs-theme') || 'light';
    const next = current === 'light' ? 'dark' : 'light';
    html.setAttribute('data-bs-theme', next);
    localStorage.setItem('aa_theme', next);
    syncIcons();
  });

  // ===== Command Palette (Ctrl/Cmd + K)
  const cmdk = document.getElementById('cmdk');
  const cmdkInput = document.getElementById('cmdk-input');
  const cmdkBtn = document.getElementById('cmdk-btn');
  const cmdkList = document.getElementById('cmdk-list');

  function openCmdk() {
    if (!cmdk) return;
    cmdk.classList.remove('d-none');
    cmdkInput?.focus();
    if (window.AOS) AOS.refreshHard();
  }
  function closeCmdk() { cmdk?.classList.add('d-none'); }

  cmdkBtn?.addEventListener('click', openCmdk);
  document.addEventListener('keydown', (e) => {
    if ((e.ctrlKey || e.metaKey) && e.key.toLowerCase() === 'k') { e.preventDefault(); openCmdk(); }
    if (e.key === 'Escape') closeCmdk();
  });
  cmdk?.addEventListener('click', (e) => { if (e.target === cmdk) closeCmdk(); });

  // Busca simples
  cmdkInput?.addEventListener('input', () => {
    const q = (cmdkInput.value || '').toLowerCase();
    cmdkList.querySelectorAll('button').forEach(btn => {
      const text = btn.textContent.toLowerCase();
      btn.classList.toggle('d-none', !text.includes(q));
    });
  });
  // Navegação ↑ ↓
  let idx = 0;
  function highlight(i) {
    const items = [...cmdkList.querySelectorAll('button:not(.d-none)')];
    items.forEach(el => el.classList.remove('active'));
    if (items.length === 0) return;
    idx = (i + items.length) % items.length;
    items[idx].classList.add('active');
    items[idx].scrollIntoView({ block: 'nearest' });
  }
  cmdkInput?.addEventListener('keydown', (e) => {
    const items = [...cmdkList.querySelectorAll('button:not(.d-none)')];
    if (!items.length) return;
    if (e.key === 'ArrowDown') { e.preventDefault(); highlight(idx + 1); }
    if (e.key === 'ArrowUp')   { e.preventDefault(); highlight(idx - 1); }
    if (e.key === 'Enter')     { e.preventDefault(); items[idx]?.click(); }
  });
  cmdkList?.addEventListener('click', (e) => {
    const btn = e.target.closest('button[data-href]');
    if (btn) window.location.href = btn.getAttribute('data-href');
  });

  // ===== Drag & Drop lite para <input type="file">
  document.querySelectorAll('form').forEach(form => {
    const input = form.querySelector('input[type="file"]');
    if (!input) return;
    // cria “drop area”
    const dz = document.createElement('div');
    dz.className = 'dropzone-lite mt-2';
    dz.innerHTML = `<div><strong>Arraste e solte</strong> o arquivo aqui ou clique para selecionar.</div>`;
    input.insertAdjacentElement('afterend', dz);
    dz.addEventListener('click', () => input.click());
    dz.addEventListener('dragover', (e) => { e.preventDefault(); dz.classList.add('dragover'); });
    dz.addEventListener('dragleave', () => dz.classList.remove('dragover'));
    dz.addEventListener('drop', (e) => {
      e.preventDefault();
      dz.classList.remove('dragover');
      if (e.dataTransfer.files?.length) {
        input.files = e.dataTransfer.files;
        // se o form tiver apenas o upload como ação principal, submete
        if (!form.dataset.noAutoSubmit) form.submit();
      }
    });
    input.addEventListener('change', () => {
      if (!form.dataset.noAutoSubmit && input.files?.length) form.submit();
    });
  });

  // ===== Contadores animados (para valores em .metric-card .val[data-count-to])
  const counters = document.querySelectorAll('.metric-card .val[data-count-to]');
  if (counters.length) {
    const obs = new IntersectionObserver((entries) => {
      entries.forEach(entry => {
        if (!entry.isIntersecting) return;
        const el = entry.target;
        const to = Number(el.getAttribute('data-count-to'));
        const dur = Number(el.getAttribute('data-count-dur') || 0.8);
        if (!isFinite(to)) return;
        const start = { v: 0 };
        gsap.to(start, {
          v: to, duration: dur, ease: 'power2.out',
          onUpdate: () => { el.textContent = start.v.toLocaleString('pt-BR', { maximumFractionDigits: 2 }); }
        });
        obs.unobserve(el);
      });
    }, { threshold: 0.5 });
    counters.forEach(c => obs.observe(c));
  }

  // ===== Toast helper
  window.aaToast = function (msg, color = 'primary') {
    const area = document.getElementById('toast-area');
    const wrapper = document.createElement('div');
    wrapper.className = 'toast align-items-center text-bg-' + color + ' border-0';
    wrapper.setAttribute('role', 'alert');
    wrapper.setAttribute('aria-live', 'assertive');
    wrapper.setAttribute('aria-atomic', 'true');
    wrapper.innerHTML = `
      <div class="d-flex">
        <div class="toast-body">${msg}</div>
        <button type="button" class="btn-close btn-close-white me-2 m-auto" data-bs-dismiss="toast"></button>
      </div>`;
    area.appendChild(wrapper);
    new bootstrap.Toast(wrapper, { delay: 3000 }).show();
  };

  // ===== Pequena animação nos cards
  document.querySelectorAll('.card').forEach((card, i) => {
    card.classList.add('reveal-up');
    card.style.animationDelay = `${Math.min(i * 0.04, 0.3)}s`;
  });

})();
