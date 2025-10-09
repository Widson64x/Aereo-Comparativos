// static/js/app.js

document.addEventListener('DOMContentLoaded', () => {
  'use strict';

  // ===== Libs Initialization =====
  function initLibs() {
    // AOS (Animate on Scroll)
    if (window.AOS) {
      AOS.init({ once: true, duration: 600, easing: 'ease-out-cubic' });
    }
    // Tippy.js (Tooltips)
    if (window.tippy) {
      tippy('[data-tippy-content]', { theme: 'light', delay: [200, 0] });
    }
  }

  // ===== Theme Toggle =====
  function setupThemeToggle() {
    const html = document.documentElement;
    const themeBtn = document.getElementById('theme-toggle');
    if (!themeBtn) return;
    
    const savedTheme = localStorage.getItem('aa_theme');
    if (savedTheme) {
      html.setAttribute('data-bs-theme', savedTheme);
    }

    const syncIcons = () => {
      const isDark = html.getAttribute('data-bs-theme') === 'dark';
      const sun = themeBtn.querySelector('.icon-sun');
      const moon = themeBtn.querySelector('.icon-moon');
      if (sun && moon) {
        sun.classList.toggle('d-none', isDark);
        moon.classList.toggle('d-none', !isDark);
      }
    };
    
    syncIcons();

    themeBtn.addEventListener('click', () => {
      const current = html.getAttribute('data-bs-theme') || 'light';
      const next = current === 'light' ? 'dark' : 'light';
      html.setAttribute('data-bs-theme', next);
      localStorage.setItem('aa_theme', next);
      syncIcons();
    });
  }

  // ===== Command Palette (Ctrl/Cmd + K) =====
  function setupCommandPalette() {
    const cmdk = document.getElementById('cmdk');
    const cmdkInput = document.getElementById('cmdk-input');
    const cmdkBtn = document.getElementById('cmdk-btn');
    const cmdkList = document.getElementById('cmdk-list');
    if (!cmdk || !cmdkInput || !cmdkBtn || !cmdkList) return;

    let activeIndex = 0;

    const openCmdk = () => {
      cmdk.classList.remove('d-none');
      cmdkInput.focus();
      updateHighlight();
      if (window.AOS) AOS.refreshHard();
    };

    const closeCmdk = () => cmdk.classList.add('d-none');

    const updateHighlight = () => {
      const items = getVisibleItems();
      items.forEach((item, i) => {
        item.classList.toggle('active', i === activeIndex);
      });
      if (items[activeIndex]) {
        items[activeIndex].scrollIntoView({ block: 'nearest' });
      }
    };

    const getVisibleItems = () => [...cmdkList.querySelectorAll('button:not(.d-none)')];

    cmdkBtn.addEventListener('click', openCmdk);
    document.addEventListener('keydown', (e) => {
      if ((e.ctrlKey || e.metaKey) && e.key.toLowerCase() === 'k') {
        e.preventDefault();
        openCmdk();
      }
      if (e.key === 'Escape') closeCmdk();
    });
    cmdk.addEventListener('click', (e) => {
      if (e.target === cmdk) closeCmdk();
    });

    cmdkInput.addEventListener('input', () => {
      const query = cmdkInput.value.toLowerCase();
      cmdkList.querySelectorAll('button').forEach(btn => {
        btn.classList.toggle('d-none', !btn.textContent.toLowerCase().includes(query));
      });
      activeIndex = 0;
      updateHighlight();
    });

    cmdkInput.addEventListener('keydown', (e) => {
      const items = getVisibleItems();
      if (!items.length) return;
      
      if (e.key === 'ArrowDown') {
        e.preventDefault();
        activeIndex = (activeIndex + 1) % items.length;
        updateHighlight();
      } else if (e.key === 'ArrowUp') {
        e.preventDefault();
        activeIndex = (activeIndex - 1 + items.length) % items.length;
        updateHighlight();
      } else if (e.key === 'Enter') {
        e.preventDefault();
        items[activeIndex]?.click();
      }
    });

    cmdkList.addEventListener('click', (e) => {
      const btn = e.target.closest('button[data-href]');
      if (btn) {
        window.location.href = btn.getAttribute('data-href');
      }
    });
  }

  // ===== Animated Counters =====
  function setupAnimatedCounters() {
    const counters = document.querySelectorAll('.metric-card .val[data-count-to]');
    if (!counters.length || !window.gsap) return;

    const observer = new IntersectionObserver((entries) => {
      entries.forEach(entry => {
        if (!entry.isIntersecting) return;
        
        const el = entry.target;
        const to = Number(el.getAttribute('data-count-to'));
        const duration = Number(el.getAttribute('data-count-dur') || 1.2);
        if (!isFinite(to)) return;

        const start = { value: 0 };
        gsap.to(start, {
          value: to,
          duration: duration,
          ease: 'power2.out',
          onUpdate: () => {
            el.textContent = Math.round(start.value).toLocaleString('pt-BR');
          }
        });
        
        observer.unobserve(el);
      });
    }, { threshold: 0.5 });

    counters.forEach(counter => observer.observe(counter));
  }

  // ===== Drag & Drop for File Inputs =====
  function setupDragAndDrop() {
      document.querySelectorAll('form').forEach(form => {
        const input = form.querySelector('input[type="file"]');
        if (!input) return;

        const dz = document.createElement('div');
        dz.className = 'dropzone-lite mt-2';
        dz.innerHTML = `<div><strong>Arraste e solte</strong> o arquivo aqui ou <strong>clique para selecionar</strong>.</div>`;
        input.insertAdjacentElement('afterend', dz);
        
        const handleFiles = (files) => {
          if (files && files.length) {
            input.files = files;
            if (!form.dataset.noAutoSubmit) {
              form.submit();
            }
          }
        };

        dz.addEventListener('click', () => input.click());
        dz.addEventListener('dragover', (e) => { e.preventDefault(); dz.classList.add('dragover'); });
        dz.addEventListener('dragleave', () => dz.classList.remove('dragover'));
        dz.addEventListener('drop', (e) => {
          e.preventDefault();
          dz.classList.remove('dragover');
          handleFiles(e.dataTransfer.files);
        });

        input.addEventListener('change', () => {
          handleFiles(input.files);
        });
      });
  }

  // ===== Toast Helper (Global) =====
  window.aaToast = function (msg, color = 'primary') {
    const area = document.getElementById('toast-area');
    if (!area) return;
    const wrapper = document.createElement('div');
    wrapper.className = `toast align-items-center text-bg-${color} border-0`;
    wrapper.setAttribute('role', 'alert');
    wrapper.setAttribute('aria-live', 'assertive');
    wrapper.setAttribute('aria-atomic', 'true');
    wrapper.innerHTML = `
      <div class="d-flex">
        <div class="toast-body">${msg}</div>
        <button type="button" class="btn-close btn-close-white me-2 m-auto" data-bs-dismiss="toast"></button>
      </div>`;
    area.appendChild(wrapper);
    const toast = new bootstrap.Toast(wrapper, { delay: 4000 });
    toast.show();
    wrapper.addEventListener('hidden.bs.toast', () => wrapper.remove());
  };

  // --- Initialize all modules ---
  initLibs();
  setupThemeToggle();
  setupCommandPalette();
  setupAnimatedCounters();
  setupDragAndDrop();
});