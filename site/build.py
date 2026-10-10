import os; os.makedirs('public',exist_ok=True)
import re,html
md=open('src/index.md').read().rstrip('\n').split('\n')
def inline(s):
    s=html.escape(s,quote=False)
    s=re.sub(r'\[([^\]]+)\]\((https?://([^)]+))\)',lambda m:f'<a href="{m.group(2)}" target="_blank" rel="noopener noreferrer"><span class="m">[</span><span class="t">{m.group(1)}</span><span class="m">]</span><span class="u">({m.group(3)})</span></a>',s)
    s=re.sub(r'\*([^*]+)\*',r'<em><span class="m">*</span>\1<span class="m">*</span></em>',s)
    return s
rows=[]
SIGNUP='''<form class="l su" id="su" method="post" action="/api/signup"><span><span class="m">&gt;</span> <input type="email" name="email" required placeholder="you@company.com" aria-label="Email address" autocomplete="email"><input type="text" name="company" tabindex="-1" autocomplete="off" class="hp" aria-hidden="true"> <button type="submit"><span class="m">[</span>Notify me<span class="m">]</span></button><span class="msg" id="msg" role="status"></span></span></form>'''
for l in md:
    if l=='{{signup}}':
        rows.append(SIGNUP); continue
    if l.startswith('## '): c=f'<span class="h2">{html.escape(l)}</span>'
    elif l.startswith('# '): c=f'<span class="h">{html.escape(l)}</span>'
    elif l.startswith('> '): c=f'<span class="m">&gt;</span> <em>{inline(l[2:])}</em>'
    elif l.startswith('- '): c=f'<span class="m">-</span> {inline(l[2:])}'
    else: c=inline(l)
    rows.append(f'<div class="l"><span>{c}</span></div>')
rows.append('<div class="l"><span></span></div>')
rows.append('<div class="l"><span><span class="cur"></span></span></div>')
t=open('template.html').read()+'<div class="doc">'
head=t.split('<div class="doc">')[0]
head=re.sub(r'<meta name="description" content="[^"]*">','<meta name="description" content="DEOOS: durable execution on object storage. Ordinary functions that survive failures, with state in your own bucket.">',head)
import re as _r
head=_r.sub(r'<link rel="icon" href="data:[^"]*">','<link rel="icon" type="image/svg+xml" href="/logo.svg"><link rel="apple-touch-icon" href="/apple-touch-icon.png">',head)
head=head.replace('<div class="tab">deoos</div>','<a href="/" style="display:inline-block;margin:0 0 1.4rem"><img src="/logo.svg" width="56" height="56" alt="DEOOS logo" style="display:block"></a><br><div class="tab">deoos</div>')
CSS="""<style>
.su input[type=email]{font:inherit;color:var(--ink);background:transparent;border:0;border-bottom:1px dashed var(--mute);padding:0 .2rem;width:min(22ch,60%);outline:none}
.su input[type=email]:focus{border-bottom-color:var(--acc)}
.su button{font:inherit;background:none;border:0;padding:0;color:var(--acc);cursor:pointer}
.su button:hover{text-decoration:underline}
.hp{position:absolute;left:-9999px;width:1px;height:1px;opacity:0}
.msg{margin-left:.6rem;color:var(--mute)}
</style>
<script>
document.addEventListener('DOMContentLoaded',()=>{const f=document.getElementById('su');if(!f)return;const m=document.getElementById('msg');
if(location.search.includes('joined=1')){m.textContent="You're on the list.";}
f.addEventListener('submit',async e=>{e.preventDefault();m.textContent='...';try{const r=await fetch('/api/signup',{method:'POST',headers:{'Accept':'application/json'},body:new FormData(f)});const j=await r.json();if(r.ok){f.querySelector('input[type=email]').value='';m.textContent="You're on the list. Thanks!";}else{m.textContent=j.error||'Something went wrong.';}}catch(_){m.textContent='Something went wrong.';}});});
</script>
"""
head=head.replace('</head>',CSS+'</head>')
open('public/index.html','w').write(head+'<div class="doc">\n'+'\n'.join(rows)+'\n</div>\n</main>\n</body>\n</html>\n')
import shutil,os
for f in ['index.md','deoos.md','llms.txt','_headers','logo.svg','apple-touch-icon.png']:
    shutil.copy(os.path.join('src',f),os.path.join('public',f))
