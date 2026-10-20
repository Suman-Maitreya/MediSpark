/* ==============================================================================
   MediSpark — app.js (Polished UI with Dock Navigation)
   macOS-style dock with proximity magnification + dark glassmorphic theme
   ============================================================================== */

const C={accent:'#00d4aa',heart:'#ff375f',diabetes:'#ff9500',stroke:'#bf5af2',lung:'#30d158',blue:'#0a84ff',text:'#f5f5f7',muted:'rgba(255,255,255,0.3)'};
let dashChart,liveDistChart,riskTrendChart,featureChart,classChart,whatifRadar;
let streamInterval=null,patients=[],currentDisease='heart';

// Normalize API keys (lungcancer -> lung_cancer)
function norm(obj){if(!obj)return {};const o={};for(const[k,v]of Object.entries(obj))o[k==='lungcancer'?'lung_cancer':k]=v;return o}
function pct(v){if(!v)return 0;if(typeof v!=='object')return v;if(v.risk_percent!==undefined)return v.risk_percent;if(v.probability!==undefined)return v.probability*100;return 0}

/* ═══════════════════════════════════════════════════════════════
   DOCK — Proximity-based magnification (inspired by React Bits)
   ═══════════════════════════════════════════════════════════════ */
const DOCK_BASE = 46;
const DOCK_MAG = 64;
const DOCK_DISTANCE = 180;

function initDock() {
    const panel = document.getElementById('dockPanel');
    const items = panel.querySelectorAll('.dock-item');

    // Set initial sizes
    items.forEach(item => {
        item.style.width = DOCK_BASE + 'px';
        item.style.height = DOCK_BASE + 'px';
        item.style.transition = 'width 0.15s ease, height 0.15s ease, background 0.2s, border-color 0.2s';
    });

    // Magnification on mouse move
    panel.addEventListener('mousemove', (e) => {
        items.forEach(item => {
            const rect = item.getBoundingClientRect();
            const center = rect.x + rect.width / 2;
            const dist = Math.abs(e.clientX - center);
            const ratio = Math.max(0, 1 - dist / DOCK_DISTANCE);
            // Smooth curve for magnification
            const scale = DOCK_BASE + (DOCK_MAG - DOCK_BASE) * Math.pow(ratio, 2);
            item.style.width = scale + 'px';
            item.style.height = scale + 'px';
        });
    });

    // Reset on mouse leave
    panel.addEventListener('mouseleave', () => {
        items.forEach(item => {
            item.style.width = DOCK_BASE + 'px';
            item.style.height = DOCK_BASE + 'px';
        });
    });

    // Click navigation
    items.forEach(item => {
        item.addEventListener('click', () => {
            const section = item.dataset.section;
            items.forEach(i => i.classList.remove('active'));
            item.classList.add('active');
            document.querySelectorAll('.section').forEach(s => s.classList.remove('active'));
            document.getElementById('sec-' + section).classList.add('active');
        });

        // Keyboard support
        item.addEventListener('keydown', (e) => {
            if (e.key === 'Enter' || e.key === ' ') {
                e.preventDefault();
                item.click();
            }
        });
    });
};

// COUNTERS
function animateCounters(){document.querySelectorAll('.card-value[data-count]').forEach(el=>{const target=parseFloat(el.dataset.count),suffix=el.dataset.suffix||'',isInt=Number.isInteger(target),dur=1200,start=performance.now();function tick(now){const p=Math.min((now-start)/dur,1),ease=1-Math.pow(1-p,4),val=target*ease;el.textContent=(isInt?Math.round(val):val.toFixed(2))+suffix;if(p<1)requestAnimationFrame(tick)}requestAnimationFrame(tick)})}

// STATUS
async function checkStatus(){try{const r=await fetch('/api/status'),d=await r.json(),b=document.getElementById('modeBadge');if(d.mode==='live'){b.className='mode-badge live';b.innerHTML='<i class="fas fa-circle"></i> LIVE'}else{b.className='mode-badge';b.innerHTML='<i class="fas fa-circle"></i> STANDALONE'}}catch(e){}}

// DASHBOARD
function initDashboard(){const ctx=document.getElementById('dashboardChart');if(!ctx)return;dashChart=new Chart(ctx,{type:'bar',data:{labels:['Heart Disease','Diabetes','Stroke','Lung Cancer','Chest X-ray'],datasets:[{label:'Records',data:[500000,500000,500000,500000,5606],backgroundColor:[C.heart+'33',C.diabetes+'33',C.stroke+'33',C.lung+'33','#5ac8fa33'],borderColor:[C.heart,C.diabetes,C.stroke,C.lung,'#5ac8fa'],borderWidth:2,borderRadius:8}]},options:{responsive:true,maintainAspectRatio:false,plugins:{legend:{display:false}},scales:{x:{ticks:{color:'rgba(255,255,255,0.4)',font:{size:11,weight:600}},grid:{color:'rgba(255,255,255,0.04)'}},y:{ticks:{color:'rgba(255,255,255,0.3)'},grid:{color:'rgba(255,255,255,0.04)'}}}}})}

// STREAMING CHARTS
function initStreamCharts(){const d=document.getElementById('liveDistChart'),t=document.getElementById('riskTrendChart');if(!d||!t)return;liveDistChart=new Chart(d,{type:'doughnut',data:{labels:['HIGH','MEDIUM','LOW'],datasets:[{data:[0,0,0],backgroundColor:[C.heart,C.diabetes,C.lung],borderWidth:0}]},options:{responsive:true,maintainAspectRatio:false,cutout:'68%',plugins:{legend:{position:'bottom',labels:{color:C.text,font:{size:11,weight:600},padding:12}}}}});riskTrendChart=new Chart(t,{type:'line',data:{labels:[],datasets:[{label:'Heart',data:[],borderColor:C.heart,fill:false,tension:.4,pointRadius:2,borderWidth:2},{label:'Diabetes',data:[],borderColor:C.diabetes,fill:false,tension:.4,pointRadius:2,borderWidth:2},{label:'Stroke',data:[],borderColor:C.stroke,fill:false,tension:.4,pointRadius:2,borderWidth:2},{label:'Lung',data:[],borderColor:C.lung,fill:false,tension:.4,pointRadius:2,borderWidth:2}]},options:{responsive:true,maintainAspectRatio:false,plugins:{legend:{labels:{color:C.text,font:{size:10,weight:600}}}},scales:{x:{ticks:{color:'rgba(255,255,255,0.3)',maxTicksLimit:10},grid:{color:'rgba(255,255,255,0.04)'}},y:{min:0,max:100,ticks:{color:'rgba(255,255,255,0.3)'},grid:{color:'rgba(255,255,255,0.04)'}}}}})}

// STREAMING
async function startStream(){try{await fetch('/api/streaming/start',{method:'POST'})}catch(e){}document.getElementById('btnStartStream').disabled=true;document.getElementById('btnStopStream').disabled=false;const s=document.getElementById('streamStatus');s.className='stream-status active';s.innerHTML='<i class="fas fa-circle"></i> Streaming';streamInterval=setInterval(pollPatients,5000);pollPatients()}
async function stopStream(){clearInterval(streamInterval);streamInterval=null;try{await fetch('/api/streaming/stop',{method:'POST'})}catch(e){}document.getElementById('btnStartStream').disabled=false;document.getElementById('btnStopStream').disabled=true;const s=document.getElementById('streamStatus');s.className='stream-status';s.innerHTML='<i class="fas fa-circle"></i> Stopped'}
async function pollPatients(){try{const r=await fetch('/api/streaming/patients'),d=await r.json();if(!d.patients||!d.patients.length)return;d.patients.forEach(p=>{patients.push(p);addPatientRow(p)});document.getElementById('patientCount').textContent=patients.length+' patients';updateStreamCharts()}catch(e){}}
function addPatientRow(p){const feed=document.getElementById('patientFeed'),ph=feed.querySelector('.placeholder');if(ph)ph.remove();const risks=norm(p.predictions||{});let badges='';for(const[dis,val]of Object.entries(risks)){const v=pct(val),lev=v>60?'HIGH':v>30?'MEDIUM':'LOW',label=dis.replace('_',' ').replace(/\b\w/g,c=>c.toUpperCase());badges+=`<span class="risk-badge ${lev}">${label} ${Math.round(v)}%</span>`}feed.insertAdjacentHTML('afterbegin',`<div class="patient-row"><span class="patient-id">${p.patient_id||'P-'+patients.length}</span><span class="patient-age">Age ${p.age||'?'}</span><span class="patient-bmi">BMI ${(p.bmi||0).toFixed(1)}</span><span class="risk-badges">${badges}</span></div>`);while(feed.children.length>50)feed.lastElementChild.remove()}
function updateStreamCharts(){let hi=0,med=0,lo=0;patients.forEach(p=>{const risks=norm(p.predictions||{});let avg=0,cnt=0;for(const v of Object.values(risks)){avg+=pct(v);cnt++}avg=cnt?avg/cnt:0;if(avg>60)hi++;else if(avg>30)med++;else lo++});if(liveDistChart){liveDistChart.data.datasets[0].data=[hi,med,lo];liveDistChart.update()}if(riskTrendChart&&patients.length>0){const last=patients[patients.length-1],risks=norm(last.predictions||{}),lbl='#'+patients.length;riskTrendChart.data.labels.push(lbl);['heart','diabetes','stroke','lung_cancer'].forEach((k,i)=>{riskTrendChart.data.datasets[i].data.push(pct(risks[k]))});if(riskTrendChart.data.labels.length>30){riskTrendChart.data.labels.shift();riskTrendChart.data.datasets.forEach(ds=>ds.data.shift())}riskTrendChart.update()}}

// CALCULATOR
async function calcPredict(){const data={age:+document.getElementById('calcAge').value,gender:+document.getElementById('calcGender').value,bmi:+document.getElementById('calcBMI').value,blood_pressure:+document.getElementById('calcBP').value,cholesterol:+document.getElementById('calcChol').value,glucose:+document.getElementById('calcGlucose').value,smoking:+document.getElementById('calcSmoking').value,exercise:+document.getElementById('calcExercise').value};try{const r=await fetch('/api/predict',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify(data)}),d=await r.json();renderCalcResults(d);document.getElementById('btnPDF').disabled=false;fetchExplanations(data)}catch(e){document.getElementById('calcResults').innerHTML='<div class="placeholder" style="color:var(--red)">Failed</div>'}}
function renderCalcResults(d){const nd=norm(d);const diseases=[{key:'heart',name:'Heart Disease',icon:'fa-heart',color:C.heart},{key:'diabetes',name:'Diabetes',icon:'fa-droplet',color:C.diabetes},{key:'stroke',name:'Stroke',icon:'fa-brain',color:C.stroke},{key:'lung_cancer',name:'Lung Cancer',icon:'fa-lungs',color:C.lung}];let html='';diseases.forEach(dis=>{const pred=nd[dis.key]||{};const p=pct(pred);const lev=p>60?'HIGH':p>30?'MEDIUM':'LOW';const lc=lev==='HIGH'?C.heart:lev==='MEDIUM'?C.diabetes:C.lung;html+=`<div class="result-card"><div class="disease-icon" style="background:${dis.color}22"><i class="fas ${dis.icon}" style="color:${dis.color}"></i></div><div class="result-info"><div class="disease-name" style="color:${dis.color}">${dis.name}</div><div class="risk-level" style="color:${lc}">${p.toFixed(1)}% — ${lev}</div><div class="risk-bar"><div class="risk-fill" style="width:${p}%;background:${lc}"></div></div></div></div>`});document.getElementById('calcResults').innerHTML=html}
async function fetchExplanations(data){try{const r=await fetch('/api/explain',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify(data)}),d=await r.json();renderExplanations(d)}catch(e){}}
function renderExplanations(d){const panel=document.getElementById('explainPanel'),grid=document.getElementById('explainGrid');const colors={heart:C.heart,diabetes:C.diabetes,stroke:C.stroke,lung_cancer:C.lung,lungcancer:C.lung};const icons={heart:'fa-heart',diabetes:'fa-droplet',stroke:'fa-brain',lung_cancer:'fa-lungs',lungcancer:'fa-lungs'};let html='';const expl=d.explanations||d;for(const[disease,info]of Object.entries(expl)){if(!info.contributions)continue;const color=colors[disease]||C.accent,icon=icons[disease]||'fa-chart-bar';const label=disease.replace(/_/g,' ').replace(/\b\w/g,c=>c.toUpperCase());html+=`<div class="explain-card"><h4><i class="fas ${icon}" style="color:${color}"></i> ${label}</h4>`;const sorted=[...info.contributions].sort((a,b)=>b.contribution-a.contribution).slice(0,6);sorted.forEach(c=>{const p=(c.contribution*100).toFixed(1),cls=c.contribution>0.15?'high':'neutral';html+=`<div class="contrib-row"><span class="contrib-feature">${c.feature}</span><span class="contrib-value">${typeof c.value==='number'?c.value.toFixed(1):c.value}</span><div class="contrib-bar-wrap"><div class="contrib-bar" style="width:${Math.min(p,100)}%;background:${color}"></div></div><span class="contrib-pct ${cls}">${p}%</span></div>`});html+='</div>'}grid.innerHTML=html;panel.style.display=html?'block':'none'}
async function downloadPDF(){const data={age:+document.getElementById('calcAge').value,gender:+document.getElementById('calcGender').value,bmi:+document.getElementById('calcBMI').value,blood_pressure:+document.getElementById('calcBP').value,cholesterol:+document.getElementById('calcChol').value,glucose:+document.getElementById('calcGlucose').value,smoking:+document.getElementById('calcSmoking').value,exercise:+document.getElementById('calcExercise').value};try{const r=await fetch('/api/report/pdf',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify(data)}),blob=await r.blob(),url=URL.createObjectURL(blob),a=document.createElement('a');a.href=url;a.download='medispark_report.pdf';a.click();URL.revokeObjectURL(url)}catch(e){alert('PDF failed')}}

// MODEL PERFORMANCE — Stage 6 results (2026-09-24, 500K rows/dataset)
const EVAL={heart:{accuracy:99.60,auc:99.99,f1:99.60,precision:99.60,recall:99.60,features:{cp:0.1777,ca:0.1420,thal:0.1281,oldpeak:0.0833,exang:0.0772,thalach:0.0732,slope:0.0660,age:0.0643,sex:0.0546,trestbps:0.0480},classes:[{label:'No Disease',precision:99.90,recall:99.32,f1:99.61},{label:'Disease',precision:99.28,recall:99.89,f1:99.59}],confusion:[[51328,349],[52,48271]]},diabetes:{accuracy:97.45,auc:99.70,f1:97.44,precision:97.47,recall:97.45,features:{Glucose:0.2938,BMI:0.1687,Age:0.1254,DiabetesPedigreeFunction:0.1037,Pregnancies:0.0923,Insulin:0.0789,SkinThickness:0.0700,BloodPressure:0.0672},classes:[{label:'No Diabetes',precision:96.86,recall:98.92,f1:97.88},{label:'Diabetes',precision:98.36,recall:95.30,f1:96.81}],confusion:[[56223,616],[1824,37007]]},stroke:{accuracy:92.13,auc:94.57,f1:89.55,precision:92.75,recall:92.13,features:{age:0.3565,avg_glucose_level:0.1472,bmi:0.1151,smoking_status_idx:0.0912,work_type_idx:0.0676,Residence_type_idx:0.0523,ever_married_idx:0.0446,gender_idx:0.0442,hypertension:0.0425,heart_disease:0.0388},classes:[{label:'No Stroke',precision:91.99,recall:100.00,f1:95.83},{label:'Stroke',precision:99.88,recall:17.89,f1:30.34}],confusion:[[90413,2],[7869,1714]]},lung_cancer:{accuracy:93.57,auc:0,f1:93.57,precision:93.58,recall:93.57,features:{Coughing_of_Blood:0.1462,Obesity:0.0852,Passive_Smoker:0.0825,Wheezing:0.0753,Clubbing_of_Finger_Nails:0.0636,Fatigue:0.0581,Balanced_Diet:0.0540,Shortness_of_Breath:0.0443,Swallowing_Difficulty:0.0436,Air_Pollution:0.0396},classes:[{label:'Medium',precision:95.34,recall:94.58,f1:94.96},{label:'High',precision:92.40,recall:94.52,f1:93.45},{label:'Low',precision:92.35,recall:90.61,f1:91.47}],confusion:[[38446,1270,932],[956,32616,934],[923,1411,22512]]},xray:{accuracy:62.30,auc:67.56,f1:61.46,precision:62.07,recall:62.30,features:{periphery_ratio:0.0916,patient_age:0.0664,center_mean:0.0528,pixel_mean:0.0502,quad_bl_mean:0.0460,quad_tl_mean:0.0449,quad_tr_mean:0.0440,quad_br_mean:0.0436,pixel_median:0.0423,center_std:0.0410},classes:[{label:'Normal',precision:63.07,recall:75.25,f1:68.62},{label:'Abnormal',precision:60.86,recall:46.61,f1:52.79}],confusion:[[444,146],[260,227]]}};

function initPerfCharts(){const f=document.getElementById('featureChart'),c=document.getElementById('classChart');if(!f||!c)return;featureChart=new Chart(f,{type:'bar',data:{labels:[],datasets:[{label:'Importance',data:[],backgroundColor:C.accent+'33',borderColor:C.accent,borderWidth:2,borderRadius:6}]},options:{indexAxis:'y',responsive:true,maintainAspectRatio:false,plugins:{legend:{display:false}},scales:{x:{ticks:{color:'rgba(255,255,255,0.3)'},grid:{color:'rgba(255,255,255,0.04)'}},y:{ticks:{color:C.text,font:{size:10,weight:600}},grid:{display:false}}}}});classChart=new Chart(c,{type:'bar',data:{labels:[],datasets:[{label:'Precision',data:[],backgroundColor:C.accent+'22',borderColor:C.accent,borderWidth:2,borderRadius:6},{label:'Recall',data:[],backgroundColor:C.blue+'22',borderColor:C.blue,borderWidth:2,borderRadius:6},{label:'F1',data:[],backgroundColor:C.stroke+'22',borderColor:C.stroke,borderWidth:2,borderRadius:6}]},options:{responsive:true,maintainAspectRatio:false,plugins:{legend:{labels:{color:C.text,font:{size:10,weight:600}}}},scales:{x:{ticks:{color:'rgba(255,255,255,0.3)'},grid:{color:'rgba(255,255,255,0.04)'}},y:{min:0,max:102,ticks:{color:'rgba(255,255,255,0.3)'},grid:{color:'rgba(255,255,255,0.04)'}}}}})}
function showDisease(disease){currentDisease=disease;document.querySelectorAll('.disease-tab').forEach(t=>t.classList.toggle('active',t.dataset.disease===disease));const d=EVAL[disease];if(!d)return;const dc={heart:C.heart,diabetes:C.diabetes,stroke:C.stroke,lung_cancer:C.lung,xray:'#5ac8fa'}[disease]||C.accent;document.getElementById('perfCards').innerHTML=[{l:'Accuracy',v:d.accuracy,i:'fa-bullseye'},{l:'AUC-ROC',v:d.auc,i:'fa-chart-area'},{l:'F1',v:d.f1,i:'fa-balance-scale'},{l:'Precision',v:d.precision,i:'fa-crosshairs'},{l:'Recall',v:d.recall,i:'fa-search'}].map(m=>`<div class="metric-card"><div class="card-icon" style="background:${dc}22"><i class="fas ${m.i}" style="color:${dc}"></i></div><div class="card-body"><div class="card-label">${m.l}</div><div class="card-value">${m.v.toFixed(2)}%</div></div></div>`).join('');if(featureChart){const entries=Object.entries(d.features).sort((a,b)=>b[1]-a[1]);featureChart.data.labels=entries.map(e=>e[0].replace(/_/g,' '));featureChart.data.datasets[0].data=entries.map(e=>e[1]);featureChart.data.datasets[0].backgroundColor=dc+'44';featureChart.data.datasets[0].borderColor=dc;featureChart.update()}if(classChart){classChart.data.labels=d.classes.map(c=>c.label);classChart.data.datasets[0].data=d.classes.map(c=>c.precision);classChart.data.datasets[1].data=d.classes.map(c=>c.recall);classChart.data.datasets[2].data=d.classes.map(c=>c.f1);classChart.update()}const cm=document.getElementById('confusionMatrix'),labels=d.classes.map(c=>c.label);let html='<table class="confusion-table"><tr><th></th>';labels.forEach(l=>html+=`<th>Pred: ${l}</th>`);html+='</tr>';d.confusion.forEach((row,i)=>{html+=`<tr><th>True: ${labels[i]}</th>`;row.forEach((v,j)=>html+=`<td class="${i===j?'diag':'off'}">${v}</td>`);html+='</tr>'});html+='</table>';cm.innerHTML=html}

// WHAT-IF
function initWhatIfChart(){const ctx=document.getElementById('whatifRadar');if(!ctx)return;whatifRadar=new Chart(ctx,{type:'radar',data:{labels:['Heart','Diabetes','Stroke','Lung Cancer'],datasets:[{label:'Risk %',data:[0,0,0,0],backgroundColor:C.accent+'15',borderColor:C.accent,borderWidth:2,pointBackgroundColor:[C.heart,C.diabetes,C.stroke,C.lung],pointRadius:5}]},options:{responsive:true,maintainAspectRatio:false,scales:{r:{min:0,max:100,ticks:{color:'rgba(255,255,255,0.3)',backdropColor:'transparent',font:{size:9}},grid:{color:'rgba(255,255,255,0.06)'},pointLabels:{color:C.text,font:{size:12,weight:700}}}},plugins:{legend:{display:false}}}})}
async function updateWhatIf(){['Age','Bmi','Bp','Chol','Gluc'].forEach(n=>{const el=document.getElementById('wi'+n),lbl=document.getElementById('wi'+n+'Val');if(el&&lbl)lbl.textContent=el.value});const data={age:+document.getElementById('wiAge').value,gender:+document.getElementById('wiGender').value,bmi:+document.getElementById('wiBmi').value,blood_pressure:+document.getElementById('wiBp').value,cholesterol:+document.getElementById('wiChol').value,glucose:+document.getElementById('wiGluc').value,smoking:+document.getElementById('wiSmoke').value};try{const r=await fetch('/api/whatif',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify(data)}),d=await r.json(),preds=norm(d),keys=['heart','diabetes','stroke','lung_cancer'],gaugeIds=['gaugeHeart','gaugeDiabetes','gaugeStroke','gaugeLung'],values=keys.map(k=>pct(preds[k]));if(whatifRadar){whatifRadar.data.datasets[0].data=values;whatifRadar.update()}keys.forEach((k,i)=>{const g=document.getElementById(gaugeIds[i]);if(!g)return;const p=values[i],lev=p>60?'HIGH':p>30?'MEDIUM':'LOW',lc=lev==='HIGH'?C.heart:lev==='MEDIUM'?C.diabetes:C.lung;g.querySelector('.gauge-value').textContent=p.toFixed(1)+'%';const badge=g.querySelector('.gauge-level');badge.textContent=lev;badge.style.background=lc+'22';badge.style.color=lc})}catch(e){}}

// BATCH
const dz=document.getElementById('batchDropZone');if(dz){dz.addEventListener('dragover',e=>{e.preventDefault();dz.classList.add('dragover')});dz.addEventListener('dragleave',()=>dz.classList.remove('dragover'));dz.addEventListener('drop',e=>{e.preventDefault();dz.classList.remove('dragover');if(e.dataTransfer.files.length)handleBatchFile(e.dataTransfer.files[0])})}
function handleBatch(input){if(input.files.length)handleBatchFile(input.files[0])}
async function handleBatchFile(file){const status=document.getElementById('batchStatus');status.innerHTML='<div class="batch-status processing"><i class="fas fa-spinner fa-spin"></i> Processing '+file.name+'...</div>';const fd=new FormData();fd.append('file',file);try{const r=await fetch('/api/batch',{method:'POST',body:fd}),d=await r.json();if(d.error){status.innerHTML='<div class="batch-status error"><i class="fas fa-times-circle"></i> '+d.error+'</div>';return}status.innerHTML='<div class="batch-status success"><i class="fas fa-check-circle"></i> '+(d.results?d.results.length:0)+' patients processed</div>';renderBatchResults(d.results||[])}catch(e){status.innerHTML='<div class="batch-status error"><i class="fas fa-times-circle"></i> Upload failed</div>'}}
function renderBatchResults(results){if(!results.length)return;document.getElementById('batchResultsPanel').style.display='block';const diseases=['heart','diabetes','stroke','lung_cancer'];let html='<table class="confusion-table"><tr><th>#</th><th>Age</th><th>BMI</th>';diseases.forEach(d=>html+=`<th>${d.replace(/_/g,' ').replace(/\b\w/g,c=>c.toUpperCase())}</th>`);html+='</tr>';results.forEach((r,i)=>{const np=norm(r.predictions||{});html+=`<tr><td>${i+1}</td><td>${r.age||'?'}</td><td>${(r.bmi||0).toFixed?.(1)||r.bmi}</td>`;diseases.forEach(d=>{const p=pct(np[d]),lev=p>60?'HIGH':p>30?'MEDIUM':'LOW',color=lev==='HIGH'?C.heart:lev==='MEDIUM'?C.diabetes:C.lung;html+=`<td style="color:${color};font-weight:700">${p.toFixed(1)}%</td>`});html+='</tr>'});html+='</table>';document.getElementById('batchResultsTable').innerHTML=html}

// X-RAY UPLOAD
const xrayDz=document.getElementById('xrayDropZone');
if(xrayDz){xrayDz.addEventListener('dragover',e=>{e.preventDefault();xrayDz.classList.add('dragover')});xrayDz.addEventListener('dragleave',()=>xrayDz.classList.remove('dragover'));xrayDz.addEventListener('drop',e=>{e.preventDefault();xrayDz.classList.remove('dragover');if(e.dataTransfer.files.length)handleXrayFile(e.dataTransfer.files[0])})}
function handleXray(input){if(input.files.length)handleXrayFile(input.files[0])}
async function handleXrayFile(file){
    const status=document.getElementById('xrayStatus');
    const preview=document.getElementById('xrayPreviewArea');
    // Show image preview
    const reader=new FileReader();
    reader.onload=function(e){
        preview.innerHTML=`<img src="${e.target.result}" class="xray-preview-img" alt="X-ray preview"><div style="font-size:12px;color:var(--text-muted);margin-top:8px">${file.name} — click to change</div>`;
    };
    reader.readAsDataURL(file);
    status.innerHTML='<div class="batch-status processing"><i class="fas fa-spinner fa-spin"></i> Analyzing X-ray...</div>';
    const fd=new FormData();
    fd.append('file',file);
    try{
        const r=await fetch('/api/xray/predict',{method:'POST',body:fd}),d=await r.json();
        if(d.error){status.innerHTML='<div class="batch-status error"><i class="fas fa-times-circle"></i> '+d.error+'</div>';return}
        status.innerHTML='<div class="batch-status success"><i class="fas fa-check-circle"></i> Analysis complete</div>';
        renderXrayResult(d);
    }catch(e){status.innerHTML='<div class="batch-status error"><i class="fas fa-times-circle"></i> Analysis failed</div>'}
}
function renderXrayResult(d){
    const pred=d.prediction||{};
    const isNormal=pred.label==='Normal';
    const prob=(pred.probability*100).toFixed(1);
    const conf=(pred.confidence*100).toFixed(0);
    const color=isNormal?C.lung:C.heart;
    const resultPanel=document.getElementById('xrayResultPanel');
    resultPanel.style.display='block';
    resultPanel.querySelector('#xrayResult').innerHTML=`
        <div class="xray-result-card">
            <div class="xray-result-label ${isNormal?'normal':'abnormal'}">
                <i class="fas ${isNormal?'fa-check-circle':'fa-exclamation-triangle'}"></i> ${pred.label}
            </div>
            <div class="xray-confidence">${conf}% confidence — Probability: ${prob}%</div>
            <div class="xray-prob-bar">
                <div class="xray-prob-fill" style="width:${prob}%;background:${color}"></div>
            </div>
            <div style="font-size:11px;color:var(--text-muted);margin-top:12px">
                Model: ${d.model||'RandomForest'} &bull; Input: ${d.image_size||'64×64'}
            </div>
        </div>`;
    // Show features
    const featPanel=document.getElementById('xrayFeaturesPanel');
    featPanel.style.display='block';
    const feats=d.features||{};
    let html='';
    const importantFeats=['pixel_mean','pixel_std','pixel_skew','pixel_kurtosis','edge_density','entropy','contrast_p90_p10','center_mean','periphery_ratio'];
    importantFeats.forEach(k=>{
        if(feats[k]!==undefined){
            html+=`<div class="xray-feature-row"><span class="feat-name">${k.replace(/_/g,' ')}</span><span class="feat-val">${feats[k].toFixed(4)}</span></div>`;
        }
    });
    featPanel.querySelector('#xrayFeatures').innerHTML=html;
}

// INIT
document.addEventListener('DOMContentLoaded',()=>{initDock();animateCounters();checkStatus();initDashboard();initStreamCharts();initPerfCharts();initWhatIfChart();showDisease('heart');updateWhatIf()});
