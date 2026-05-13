import asyncio, csv, html as ihtml, itertools, json, pathlib, re, shutil, sys, time, urllib.parse
from urllib.parse import urljoin
import requests, websockets
from bs4 import BeautifulSoup

sys.stdout.reconfigure(encoding='utf-8')
BASE = pathlib.Path(r'C:\Users\Playdata\Desktop\CSV\mfk')
FRESH = BASE / 'fresh_run'
HTML_DIR = FRESH / 'html'
CATEGORIES = [
    ('unisex', 'https://www.franciskurkdjian.com/int-en/fragrances/unisex-fragrances/'),
    ('women', 'https://www.franciskurkdjian.com/int-en/fragrances/womens-fragrances/'),
    ('men', 'https://www.franciskurkdjian.com/int-en/fragrances/mens-fragrances/'),
]
FINAL_JSON = BASE / 'maison_francis_kurkdjian_fragrances.json'
FINAL_CSV = BASE / 'maison_francis_kurkdjian_fragrances.csv'
RAW_JSON = FRESH / 'maison_francis_kurkdjian_fresh_raw.json'
LINKS_JSON = FRESH / 'fresh_links_rows.json'
DETAILS_JSON = FRESH / 'fresh_details_by_url.json'
EXCLUDED_JSON = FRESH / 'excluded_sets_duos_samples_refills.json'
LOG = FRESH / 'fresh_run.log'

EXCLUDE_PATTERNS = ['duo', 'set', 'sample', 'refill', 'refills', 'wardrobe']
COLS = ['country','korean_name','english_name','product_type','product_url','regular_price','image_url','ingredients','key_ingredients']

name_ko = {
'Baccarat Rouge 540':'바카라 루쥬 540','OUD satin mood':'우드 사틴 무드','Aqua Universalis':'아쿠아 유니버설리스','Kurky':'커키','Grand Soir':'그랑 수아르','Petit Matin':'쁘띠 마탱','OUD':'우드','OUD silk mood':'우드 실크 무드','gentle Fluidity':'젠틀 플루이디티','Gentle fluidity':'젠틀 플루이디티','724':'724','Le Beau Parfum':'르 보 퍼퓸','Absolue Pour le Soir':'압솔뤼 뿌르 르 수아르','Absolue Pour le Matin':'압솔뤼 뿌르 르 마탱','Reflets d ambre':'르플레 담브르',"Reflets d'ambre":'르플레 담브르','APOM':'아폼','Aqua Universalis Cologne forte':'아쿠아 유니버설리스 코롱 포르테','Aqua Media Cologne forte':'아쿠아 미디어 코롱 포르테','Aqua Celestia':'아쿠아 셀레스티아','Aqua Celestia Cologne forte':'아쿠아 셀레스티아 코롱 포르테','À la rose':'아 라 로즈','l eau À la rose':'로 아 라 로즈',"l'eau À la rose":'로 아 라 로즈','Amyris femme':'아미리스 팜므','féminin Pluriel':'페미닌 플루리엘','l’Homme À la rose':'롬므 아 라 로즈',"l'Homme À la rose":'롬므 아 라 로즈','Amyris homme':'아미리스 옴므','masculin Pluriel':'마스큘린 플루리엘'}
type_ko = {'Eau de parfum':'오 드 퍼퓸','Extrait de parfum':'엑스트레 드 퍼퓸','Eau de toilette':'오 드 뚜왈렛','Eau parfumée':'오 파르퓨메','Gold Edition - Eau de parfum':'골드 에디션 - 오 드 퍼퓸','Silver Edition - Eau de parfum':'실버 에디션 - 오 드 퍼퓸','Precious Elixir':'프레셔스 엘릭서'}
note_ko = {'Hedione':'헤디온','AmbroxanTM':'암브록산TM','Saffron':'사프란','Violet':'바이올렛','Oud':'우드','Damascena Rose':'다마세나 로즈','Vanilla Amber Accord':'바닐라 앰버 어코드','Benzoin':'벤조인','Musks':'머스크','Bergamot':'베르가못','Bouquet of fresh white flowers':'프레시 화이트 플라워 부케','Lemon':'레몬','Gourmand musky accord':'구르망 머스키 어코드','Tutti-frutti accord':'투티 프루티 어코드','Cashmeran':'캐시메란','Grandiflorum Jasmine':'그란디플로럼 자스민','Cinnamon tree':'시나몬 트리','Ciste Labdanum':'시스트 라브다넘','Lavender':'라벤더','Litsea Cubeba':'리치아 쿠베바','Hawthorn':'호손','Orange blossom':'오렌지 블로섬','Patchouli':'패출리','Cedar':'시더','Elemi':'엘레미','Vanilla':'바닐라','Geranium':'제라늄','Centifolia Rose':'센티폴리아 로즈','Cinnamon':'시나몬','Papyrus':'파피루스','Guaiac wood':'과이악 우드','Blue Chamomile':'블루 캐모마일','Nutmeg':'넛맥','Juniper Berries':'주니퍼 베리','Amber woods':'앰버 우드','Coriander seeds':'코리앤더 씨드','White Sandalwood':'화이트 샌달우드','Aldehydes':'알데하이드','Ylang Ylang':'일랑일랑','Tuberose':'튜베로즈','Rose-Honey accord':'로즈 허니 어코드','Cumin oil':'커민 오일','Powdery Woody Accord':'파우더리 우디 어코드','Thyme':'타임','Leathery amber accord':'레더리 앰버 어코드','Pink peppercorns':'핑크 페퍼콘','Ambery Woody Accord':'앰버리 우디 어코드','Verbena accord':'버베나 어코드','Essence of sweet fennel':'스위트 펜넬 에센스','Mimosa':'미모사','Blackcurrant buds from Burgundy':'부르고뉴 블랙커런트 버드','Lime':'라임','Mitcham mint':'미첨 민트','Sweet pea':'스위트피','Peony':'피오니','Lychee':'리치','Pear':'배','Amyris':'아미리스','Lemon blossom':'레몬 블로섬','Iris':'아이리스','Mandarin blossom':'만다린 블로섬','Vetiver':'베티버','Mandarin':'만다린','Rosemary':'로즈마리','Clary Sage':'클라리 세이지','Grapefruit':'자몽','Tonka bean':'통카빈','Leather accord':'레더 어코드'}

def log(msg):
    print(msg, flush=True)
    with LOG.open('a', encoding='utf-8') as f:
        f.write(msg + '\n')

def clean(s):
    return re.sub(r'\s+', ' ', (s or '').replace('<br>', ' ')).strip()

def canonical_url(u):
    u = urllib.parse.unquote(u or '')
    u = re.sub(r'<br\s*/?>', '-', u, flags=re.I)
    u = re.sub(r'\s+', '', u)
    return u

def reset_outputs():
    BASE.mkdir(parents=True, exist_ok=True)
    for p in [FINAL_JSON, FINAL_CSV]:
        if p.exists():
            p.unlink()
    if FRESH.exists():
        shutil.rmtree(FRESH)
    HTML_DIR.mkdir(parents=True, exist_ok=True)
    log('RESET_DONE')

async def cdp(ws, method, params=None, counter=itertools.count(1)):
    i = next(counter)
    await ws.send(json.dumps({'id': i, 'method': method, 'params': params or {}}))
    while True:
        msg = json.loads(await ws.recv())
        if msg.get('id') == i:
            return msg

async def nav_html(ws, url, settle=4, scroll=True):
    await cdp(ws, 'Page.navigate', {'url': url})
    last = ''
    for _ in range(75):
        res = await cdp(ws, 'Runtime.evaluate', {'expression':'location.href+"||"+document.readyState+"||"+document.title', 'returnByValue': True})
        last = res['result']['result'].get('value','')
        if url.split('#')[0] in last and 'complete' in last:
            break
        time.sleep(1)
    time.sleep(settle)
    if scroll:
        for y in [0, 800, 1600, 2800, 4500, 7000, 10000, 14000, 19000, 25000, 33000, 43000, 56000]:
            await cdp(ws, 'Runtime.evaluate', {'expression': f'window.scrollTo(0,{y});', 'returnByValue': True})
            time.sleep(0.65)
        await cdp(ws, 'Runtime.evaluate', {'expression':'window.scrollTo(0, document.body.scrollHeight);', 'returnByValue': True})
        time.sleep(2)
    res = await cdp(ws, 'Runtime.evaluate', {'expression':'document.documentElement.outerHTML', 'returnByValue': True})
    html = res['result']['result'].get('value','')
    if 'Access Denied' in html[:2000]:
        raise RuntimeError('Access Denied')
    return html, last

def parse_category(html, category):
    soup = BeautifulSoup(html, 'html.parser')
    rows, excluded = [], []
    for div in soup.select('div.product[data-pid]'):
        a = div.select_one('a[href*="/int-en/p/"]')
        if not a:
            continue
        gtm = div.get('data-gtm-impression') or a.get('data-gtm-ecommerce-product-impression') or '{}'
        try:
            data = json.loads(ihtml.unescape(gtm))
        except Exception:
            data = {}
        name = clean(data.get('dimension23') or '')
        line = clean(data.get('dimension7') or '')
        if not name:
            n = div.select_one('.mfk_productName') or div.select_one('[id^="ada-product-title"]')
            name = clean(n.get_text(' ', strip=True) if n else '')
        if not line:
            t = div.select_one('.mfk_commercialLineName') or div.select_one('[id^="ada-product-type"]')
            line = clean(t.get_text(' ', strip=True) if t else '')
        url = canonical_url(urljoin('https://www.franciskurkdjian.com', a.get('href')))
        img = div.select_one('img[src], img[data-src]')
        item = {
            'source_category': category,
            'english_name': name,
            'product_type_en': line,
            'product_url': url,
            'image_url_cat': canonical_url((img.get('data-src') or img.get('src')) if img else ''),
            'variant': div.get('data-defaultvariant') or div.get('data-firstvariant') or data.get('id',''),
        }
        hay = ' '.join([name, line, url]).lower()
        if any(p in hay for p in EXCLUDE_PATTERNS):
            excluded.append(item)
        else:
            rows.append(item)
    return rows, excluded

def parse_detail(html):
    soup = BeautifulSoup(html, 'html.parser')
    ld = {}
    for s in soup.select('script[type="application/ld+json"]'):
        try:
            obj = json.loads(s.string or s.get_text())
            if obj.get('@type') == 'Product':
                ld = obj; break
        except Exception:
            pass
    ing_el = soup.select_one('.ingredients-list') or soup.select_one('#product-ingredients .ingredients-list')
    notes = []
    for ni in soup.select('#product-notes .notes-item'):
        h = ni.select_one('h2.second-title')
        if h:
            t = clean(h.get_text(' ', strip=True)).replace(' TM', 'TM')
            if t and t not in notes:
                notes.append(t)
    offers = ld.get('offers') or {}
    price = ''
    if isinstance(offers, dict) and offers.get('price'):
        try: price = f"₩ {int(float(offers.get('price'))):,}"
        except Exception: price = str(offers.get('price'))
    imgs = ld.get('image') or []
    image = ''
    if isinstance(imgs, list) and imgs:
        image = imgs[0].get('url') if isinstance(imgs[0], dict) else str(imgs[0])
    elif isinstance(imgs, str):
        image = imgs
    return {
        'ld_name': clean(ld.get('name','')),
        'sku': ld.get('sku',''),
        'size': ld.get('size',''),
        'regular_price': price,
        'image_url': canonical_url(image),
        'ingredients': clean(ing_el.get_text(' ', strip=True) if ing_el else ''),
        'key_ingredients_en': notes,
    }

def normalize(rows, details_by_url):
    out = []
    for r in rows:
        d = details_by_url.get(r['product_url'], {})
        en = clean(r['english_name'])
        ptype_en = clean(r['product_type_en'])
        out.append({
            'country': 'KR',
            'korean_name': clean((name_ko.get(en, en) + ' ' + type_ko.get(ptype_en, ptype_en)).strip()),
            'english_name': en,
            'product_type': type_ko.get(ptype_en, ptype_en),
            'product_url': r['product_url'],
            'regular_price': d.get('regular_price',''),
            'image_url': d.get('image_url') or r.get('image_url_cat',''),
            'ingredients': d.get('ingredients',''),
            'key_ingredients': [note_ko.get(n, n) for n in d.get('key_ingredients_en', [])],
        })
    return out

async def main():
    reset_outputs()
    targets = requests.get('http://127.0.0.1:9222/json/list', timeout=5).json()
    page = next(t for t in targets if t.get('type') == 'page')
    async with websockets.connect(page['webSocketDebuggerUrl'], max_size=180_000_000) as ws:
        await cdp(ws, 'Page.enable'); await cdp(ws, 'Runtime.enable')
        rows, excluded = [], []
        for category, url in CATEGORIES:
            log(f'CATEGORY_START {category} {url}')
            html, state = await nav_html(ws, url, settle=5, scroll=True)
            (HTML_DIR / f'category_{category}.html').write_text(html, encoding='utf-8')
            parsed, ex = parse_category(html, category)
            rows.extend(parsed); excluded.extend(ex)
            log(f'CATEGORY_DONE {category} kept={len(parsed)} excluded={len(ex)} state={state}')
        LINKS_JSON.write_text(json.dumps(rows, ensure_ascii=False, indent=2), encoding='utf-8')
        EXCLUDED_JSON.write_text(json.dumps(excluded, ensure_ascii=False, indent=2), encoding='utf-8')
        unique_urls = []
        for r in rows:
            if r['product_url'] not in unique_urls:
                unique_urls.append(r['product_url'])
        log(f'INDEX_DONE mixed_rows={len(rows)} unique_detail_urls={len(unique_urls)} excluded={len(excluded)}')
        details = {}
        for idx, url in enumerate(unique_urls, 1):
            log(f'DETAIL_START {idx}/{len(unique_urls)} {url}')
            html, state = await nav_html(ws, url, settle=5, scroll=True)
            safe = re.sub(r'[^A-Za-z0-9_-]+','_', url.rsplit('/',1)[-1].replace('.html',''))[:90]
            (HTML_DIR / f'pdp_{idx:03d}_{safe}.html').write_text(html, encoding='utf-8')
            details[url] = parse_detail(html)
            DETAILS_JSON.write_text(json.dumps(details, ensure_ascii=False, indent=2), encoding='utf-8')
            log(f'DETAIL_DONE {idx}/{len(unique_urls)} price={bool(details[url].get("regular_price"))} ing={bool(details[url].get("ingredients"))} notes={len(details[url].get("key_ingredients_en", []))} state={state}')
            time.sleep(1.2)
        final_rows = normalize(rows, details)
        RAW_JSON.write_text(json.dumps({'rows': rows, 'details_by_url': details, 'final_rows': final_rows}, ensure_ascii=False, indent=2), encoding='utf-8')
        FINAL_JSON.write_text(json.dumps(final_rows, ensure_ascii=False, indent=2), encoding='utf-8')
        with FINAL_CSV.open('w', encoding='utf-8-sig', newline='') as f:
            w = csv.DictWriter(f, fieldnames=COLS)
            w.writeheader()
            for r in final_rows:
                rr = r.copy(); rr['key_ingredients'] = json.dumps(rr['key_ingredients'], ensure_ascii=False)
                w.writerow(rr)
        bad_url = sum('%3C' in r['product_url'] or '<br' in r['product_url'].lower() for r in final_rows)
        bad_excluded = sum(any(p in (' '.join([r['english_name'], r['product_type'], r['product_url']]).lower()) for p in EXCLUDE_PATTERNS) for r in final_rows)
        log(f'EXPORT_DONE rows={len(final_rows)} unique_urls={len(set(r["product_url"] for r in final_rows))} missing_price={sum(not r["regular_price"] for r in final_rows)} missing_ing={sum(not r["ingredients"] for r in final_rows)} missing_notes={sum(not r["key_ingredients"] for r in final_rows)} bad_url={bad_url} bad_excluded={bad_excluded}')
        log(f'OUTPUT_CSV {FINAL_CSV}')
        log(f'OUTPUT_JSON {FINAL_JSON}')

if __name__ == '__main__':
    asyncio.run(main())
