// C:\Programs\Aéreo-Comparativos\static\js\kpi_map.js  (v13)
(function () {
  const KPIMap = {
    _map:null, _nodes:null, _links:null, _decor:null, _data:null, _ro:null,
    _controls:{}, _batchId:null, _endpoints:{},

    init(elId, dataUrl, batchId){
      this._batchId = batchId;
      this._endpoints = {
        routeItems: (o,d,q)=> `/aereo-comparativos/kpi/map/route/items/${batchId}?o=${encodeURIComponent(o)}&d=${encodeURIComponent(d)}${q||""}`,
        nodeSummary: (iata)=> `/aereo-comparativos/kpi/map/node/summary/${batchId}?iata=${encodeURIComponent(iata)}`,
        nodeItems: (iata,dir)=> `/aereo-comparativos/kpi/map/node/items/${batchId}?iata=${encodeURIComponent(iata)}&dir=${dir||"in"}`
      };

      if(this._map){ this.invalidate(); return; }
      const el=document.getElementById(elId); if(!el) return;

      this._map=L.map(elId,{preferCanvas:true,zoomControl:true});
      L.tileLayer('https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png',{maxZoom:10,attribution:'&copy; OpenStreetMap'}).addTo(this._map);
      this._nodes=L.layerGroup().addTo(this._map);
      this._links=L.layerGroup().addTo(this._map);
      this._decor=L.layerGroup().addTo(this._map);

      this._controls = {
        scaleCount:document.getElementById('scaleCount'),
        scaleFrete:document.getElementById('scaleFrete'),
        toggleNodes:document.getElementById('toggleNodes'),
        toggleLinks:document.getElementById('toggleLinks'),
        fDev:document.getElementById('fDev'),
        fSem:document.getElementById('fSem'),
        fMin:document.getElementById('fMin'),
        fPex:document.getElementById('fPex'),
        fDif:document.getElementById('fDif'),
        fEnv:document.getElementById('fEnv'),
      };
      const R=()=>{ this.render(); this.fit(); };
      Object.values(this._controls).forEach(c=>c&&c.addEventListener('change',R));

      window.addEventListener('resize',()=>this.invalidate());
      this._ro=new ResizeObserver(()=>this.invalidate()); this._ro.observe(el);

      fetch(dataUrl).then(r=>r.json()).then(j=>{ this._data=j; this.render(); this.fit(); setTimeout(()=>this.fit(),60); setTimeout(()=>this.fit(),300); });
    },

    invalidate(){ if(this._map){ this._map.invalidateSize(); } },

    _money(v){ if(v==null||isNaN(v)) return 'R$ 0,00'; return 'R$ '+Number(v).toLocaleString('pt-BR',{minimumFractionDigits:2,maximumFractionDigits:2}); },

    _lineWeight(l){
      if(this._controls.scaleFrete?.checked){ const v=Math.max(1,Math.log10(Math.max(1,l.sum_frete))); return Math.min(12,1+v*2.2); }
      return Math.min(12,1+Math.sqrt(Math.max(1,l.count)));
    },

    // filtros marcados
    _chosen(){
      const a=[];
      if(this._controls.fDev?.checked) a.push('DEVOLUCAO');
      if(this._controls.fSem?.checked) a.push('TARIFA NAO LOCALIZADA');
      if(this._controls.fMin?.checked) a.push('FRETE MINIMO');
      if(this._controls.fPex?.checked) a.push('PESO EXCEDENTE');
      if(this._controls.fDif?.checked) a.push('COM_DIF');
      return a;
    },

    // passa no filtro? OR entre os marcados; “Só envios” exclui rotas com devolução
    _passStatusFilters(l){
      const chosen=this._chosen();
      const st=l.status||{};
      if(this._controls.fEnv?.checked && (st['DEVOLUCAO']||0)>0) return false;
      if(chosen.length===0) return true;
      return chosen.some(k => (st[k]||0) > 0);
    },

    // cor: se houver filtros ativos, usa o dominante entre eles; caso contrário usa prioridade fixa
    _statusColor(l, chosen){
      const st=l.status||{};
      const palette={
        'DEVOLUCAO':'#ef4444',
        'TARIFA NAO LOCALIZADA':'#f59e0b',
        'FRETE MINIMO':'#a855f7',
        'PESO EXCEDENTE':'#6366f1',
        'COM_DIF':'#3b82f6',
        '_default':'#14b8a6'
      };

      if(chosen && chosen.length){
        let best=null, bestCnt=0;
        for(const k of chosen){
          const c=st[k]||0;
          if(c>bestCnt){ bestCnt=c; best=k; }
        }
        return best ? palette[best] : palette._default;
      }

      if((st['DEVOLUCAO']||0)>0) return palette['DEVOLUCAO'];
      if((st['TARIFA NAO LOCALIZADA']||0)>0) return palette['TARIFA NAO LOCALIZADA'];
      if((st['FRETE MINIMO']||0)>0) return palette['FRETE MINIMO'];
      if((st['PESO EXCEDENTE']||0)>0) return palette['PESO EXCEDENTE'];
      if((st['COM_DIF']||0)>0) return palette['COM_DIF'];
      return palette._default;
    },

    // garante existência do modal
    _ensureModal(){
      let modalEl=document.getElementById('kpiRouteModal');
      if(modalEl) return modalEl;
      const tpl=document.createElement('div');
      tpl.innerHTML = `
      <div class="modal fade" id="kpiRouteModal" tabindex="-1" aria-hidden="true">
        <div class="modal-dialog modal-xl modal-dialog-scrollable"><div class="modal-content">
          <div class="modal-header">
            <h5 class="modal-title">Itens</h5>
            <button type="button" class="btn-close" data-bs-dismiss="modal" aria-label="Fechar"></button>
          </div>
          <div class="modal-body"><div class="text-muted">Carregando…</div></div>
        </div></div>
      </div>`;
      document.body.appendChild(tpl.firstElementChild);
      return document.getElementById('kpiRouteModal');
    },

    render(){
      if(!this._map||!this._data) return;
      this._nodes.clearLayers(); this._links.clearLayers(); this._decor.clearLayers();

      // nós
      if(this._controls.toggleNodes?.checked){
        this._data.nodes.forEach(n=>{
          const r=Math.max(4,Math.min(18,Math.sqrt(n.in_count+n.out_count)));
          const m=L.circleMarker([n.lat,n.lon],{radius:r,weight:1,opacity:1,fillOpacity:0.6});
          m.bindPopup(
            `<div><strong>${n.iata}</strong> — ${n.name||'(sem nome)'}</div>
             <div>${n.region||''} • ${n.country||''}</div>
             <hr class="my-1"/>
             <div>Recebido: <b>${n.in_count}</b> • Enviado: <b>${n.out_count}</b></div>
             <div>Total Frete: <b>${this._money(n.sum_frete)}</b></div>
             <div class="mt-2">
               <button class="btn btn-sm btn-outline-primary" data-node="${n.iata}" data-dir="in">Ver recebidos</button>
               <button class="btn btn-sm btn-outline-secondary ms-1" data-node="${n.iata}" data-dir="out">Ver enviados</button>
             </div>`
          );
          m.on('popupopen',(e)=>{
            const container=e.popup?.getElement?.() || document;
            container.querySelectorAll(`button[data-node="${n.iata}"]`).forEach(b=>{
              b.addEventListener('click',()=>{
                const dir=b.getAttribute('data-dir')||'in';
                this._openModal(
                  `Itens ${dir==='in'?'recebidos em':'enviados de'} ${n.iata}`,
                  this._endpoints.nodeItems(n.iata,dir)
                );
              });
            });
          });
          this._nodes.addLayer(m);
        });
      }

      // arestas
      if(this._controls.toggleLinks?.checked){
        const chosenFilters=this._chosen();
        this._data.links.forEach(l=>{
          if(!this._passStatusFilters(l)) return;

          const color=this._statusColor(l, chosenFilters);
          const latlngs=[[l.o_lat,l.o_lon],[l.d_lat,l.d_lon]];
          const w=this._lineWeight(l);

          // glow
          const glow=L.polyline(latlngs,{weight:Math.max(6,w+4), opacity:.55, color:'#ffffff', className:'route-glow'});
          this._links.addLayer(glow);

          // linha principal
          const cls = this._controls.fEnv?.checked ? 'route-main route-anim' : 'route-main';
          const pl=L.polyline(latlngs,{weight:w,opacity:.9,color, className:cls});
          const st=Object.entries(l.status||{}).map(([k,v])=>`<div>${k}: <b>${v}</b></div>`).join('');
          const html=`<div><strong>${l.o} → ${l.d}</strong></div>
             <div>Docs: <b>${l.count}</b></div>
             <div>Frete: soma <b>${this._money(l.sum_frete)}</b> • média <b>${this._money(l.avg_frete)}</b></div>
             <div>Tarifa: soma <b>${this._money(l.sum_tarifa)}</b> • média <b>${this._money(l.avg_tarifa)}</b></div>
             <div>Peso: soma <b>${l.sum_peso?.toLocaleString('pt-BR')}</b> • média <b>${l.avg_peso?.toLocaleString('pt-BR')}</b></div>
             ${st?`<hr class="my-1" />${st}`:''}
             <div class="mt-2">
               <button class="btn btn-sm btn-primary" data-route="open" data-o="${l.o}" data-d="${l.d}">Ver itens da rota</button>
             </div>`;
          pl.bindPopup(html);
          pl.on('popupopen',(e)=>{
            const container=e.popup?.getElement?.() || document;
            const btn=container.querySelector('button[data-route="open"]');
            if(btn){
              btn.addEventListener('click',()=>{
                const o=btn.getAttribute('data-o'), d=btn.getAttribute('data-d');
                const q=this._buildRouteQuery();
                this._openModal(`Itens da rota ${o} → ${d}`, this._endpoints.routeItems(o,d,q));
              });
            }
          });
          this._links.addLayer(pl);

          // setas destino quando “Só envios”
          if(this._controls.fEnv?.checked && window.L && L.polylineDecorator){
            const patterns=['25%','50%','75%'].map(offset=>({
              offset, repeat:0,
              symbol:L.Symbol.arrowHead({pixelSize:10,headAngle:55,pathOptions:{color,fillOpacity:1,weight:1,opacity:.95}})
            }));
            const deco=L.polylineDecorator(latlngs,{patterns});
            this._decor.addLayer(deco);
          }
        });
      }
    },

    _buildRouteQuery(){
      const p = new URLSearchParams();
      if(this._controls.fDev?.checked) p.set('dev','1');
      if(this._controls.fSem?.checked) p.set('sem','1');
      if(this._controls.fMin?.checked) p.set('min','1');
      if(this._controls.fPex?.checked) p.set('pex','1');
      if(this._controls.fDif?.checked) p.set('diff','1');
      return p.toString() ? '&'+p.toString() : '';
    },

    _openModal(title, url){
      const modalEl=this._ensureModal();
      const ttl=modalEl.querySelector('.modal-title');
      const body=modalEl.querySelector('.modal-body');
      if(ttl) ttl.textContent=title;
      if(body) body.innerHTML='<div class="text-muted">Carregando…</div>';

      fetch(url).then(r=>r.text()).then(html=>{ if(body) body.innerHTML=html; });

      if(window.bootstrap && bootstrap.Modal){
        const bsModal = bootstrap.Modal.getOrCreateInstance(modalEl);
        bsModal.show();
      }else{
        modalEl.style.display='block';
      }
    },

    fit(){
      if(!this._map||!this._data) return;
      const pts=[];
      if(this._data.nodes?.length){ this._data.nodes.forEach(n=>pts.push([n.lat,n.lon])); }
      else if(this._data.links?.length){ this._data.links.forEach(l=>{ pts.push([l.o_lat,l.o_lon],[l.d_lat,l.d_lon]); }); }
      this._map.invalidateSize();
      if(pts.length){ try{ this._map.fitBounds(pts,{padding:[20,20]}); }catch{ this._map.setView([-14.2350,-51.9253],4); } }
      else{ this._map.setView([-14.2350,-51.9253],4); }
    }
  };

  window.KPIMap = KPIMap;
})();
