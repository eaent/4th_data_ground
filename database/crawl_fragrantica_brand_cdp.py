#!/usr/bin/env python3
import csv
import html as ihtml
import json
import random
import re
import sqlite3
import sys
import time
from pathlib import Path
from playwright.sync_api import sync_playwright

FORBIDDEN = [
    '100% Free - Always',
    'Create Free Account',
    'Already have an account? Log in',
    'Continue browsing',
]
COLS = ['brand', 'name', 'release_year', 'accords', 'top_notes', 'middle_notes', 'base_notes']
CDP_URL = 'http://127.0.0.1:9222'

def clean_text(s: str) -> str:
    s = re.sub(r'<script[\s\S]*?</script>|<style[\s\S]*?</style>', ' ', s, flags=re.I)
    s = re.sub(r'<[^>]+>', ' ', s)
    s = ihtml.unescape(s)
    s = re.sub(r'\s+', ' ', s).strip()
    for bad in FORBIDDEN:
        s = s.replace(bad, '').strip()
    return s

def extract_name(page_html: str, url: str, brand: str) -> str:
    m = re.search(r'<h1[^>]*itemprop=["\']name["\'][^>]*>([\s\S]*?)</h1>', page_html, re.I)
    if m:
        txt = clean_text(m.group(1))
        txt = re.sub(r'\s+for\s+(women|men|women and men|men and women)\s*$', '', txt, flags=re.I).strip()
        txt = re.sub(r'\s+' + re.escape(brand).replace('\\ ', r'\s+') + r'\s*$', '', txt, flags=re.I).strip()
        txt = re.sub(r'\s+by\s+' + re.escape(brand).replace('\\ ', r'\s+') + r'\s*$', '', txt, flags=re.I).strip()
        if txt:
            return txt
    slug = url.rsplit('/', 1)[-1].replace('.html', '')
    slug = re.sub(r'-\d+$', '', slug)
    return slug.replace('-', ' ')

def extract_description_text(page_html: str) -> str:
    m = re.search(r'<div[^>]+id=["\']perfume-description-content["\'][^>]*>([\s\S]*?)(?:<pyramid-level-new|</section>|</article>)', page_html, re.I)
    if m:
        return clean_text(m.group(1))
    return clean_text(page_html[:300000])

def extract_year(desc: str) -> str:
    for pat in [r'was launched in\s+(\d{4})', r'were launched in\s+(\d{4})', r'launched in\s+(\d{4})', r'introduced in\s+(\d{4})']:
        m = re.search(pat, desc, re.I)
        if m:
            return m.group(1)
    m = re.search(r'\b(19\d{2}|20\d{2})\b', desc[:1200])
    return m.group(1) if m else ''

def extract_accords(page_html: str):
    idx = page_html.lower().find('main accords')
    if idx == -1:
        return []
    end_candidates = [p for p in [page_html.find('perfume-description-content', idx), page_html.find('<pyramid-level-new', idx)] if p != -1]
    end = min(end_candidates) if end_candidates else idx + 20000
    seg = page_html[idx:end]
    vals = [clean_text(x) for x in re.findall(r'<span[^>]*class=["\'][^"\']*\btruncate\b[^"\']*["\'][^>]*>([\s\S]*?)</span>', seg, re.I)]
    out = []
    for v in vals:
        if v and v.lower() != 'main accords' and v not in out and not any(b in v for b in FORBIDDEN):
            out.append(v)
    return out

def extract_notes_level(page_html: str, level: str):
    m = re.search(r'<pyramid-level-new\s+notes=["\']' + re.escape(level) + r'["\'][^>]*>([\s\S]*?)</pyramid-level-new>', page_html, re.I)
    if not m:
        return []
    seg = m.group(1)
    vals = [clean_text(x) for x in re.findall(r'<span[^>]*class=["\'][^"\']*pyramid-note-label[^"\']*["\'][^>]*>([\s\S]*?)</span>', seg, re.I)]
    if not vals:
        vals = [ihtml.unescape(x).strip() for x in re.findall(r'alt=["\']([^"\']+)["\']', seg, re.I)]
    out = []
    for v in vals:
        if v and v not in out and not any(b in v for b in FORBIDDEN):
            out.append(v)
    return out

def split_note_sentence(desc: str):
    result = {'top': [], 'middle': [], 'base': []}
    patterns = [
        r'Top notes? (?:are|is) (.*?);\s*middle notes? (?:are|is) (.*?);\s*base notes? (?:are|is) (.*?)(?:\.|$)',
        r'Top notes?:? (.*?);\s*Middle notes?:? (.*?);\s*Base notes?:? (.*?)(?:\.|$)',
    ]
    for pat in patterns:
        m = re.search(pat, desc, re.I)
        if m:
            for key, val in zip(['top', 'middle', 'base'], m.groups()):
                result[key] = [x.strip() for x in re.split(r',| and ', val) if x.strip()]
            break
    return result

def extract_feature_notes(desc: str):
    patterns = [r'The fragrance features\s+(.*?)(?:\.|$)', r'features\s+(.*?)(?:\.|$)', r'notes include\s+(.*?)(?:\.|$)']
    for pat in patterns:
        m = re.search(pat, desc, re.I)
        if m:
            raw = m.group(1)
            notes = [x.strip() for x in re.split(r',| and ', raw) if x.strip()]
            return [n for n in notes if not any(b in n for b in FORBIDDEN)]
    return []

def parse_perfume(url: str, page_html: str, brand: str):
    desc = extract_description_text(page_html)
    top = extract_notes_level(page_html, 'top')
    middle = extract_notes_level(page_html, 'middle')
    base = extract_notes_level(page_html, 'base')
    if not (top and middle and base):
        fallback = split_note_sentence(desc)
        top = top or fallback['top']
        middle = middle or fallback['middle']
        base = base or fallback['base']
    if not top and not middle and not base:
        base = extract_feature_notes(desc)
    return {
        'brand': brand,
        'name': extract_name(page_html, url, brand),
        'release_year': extract_year(desc),
        'accords': '; '.join(extract_accords(page_html)),
        'top_notes': '; '.join(top),
        'middle_notes': '; '.join(middle),
        'base_notes': '; '.join(base),
        '_url': url,
    }

def write_outputs(out_dir: Path, file_base: str, results: list):
    public_rows = [{c: (r.get(c, '') or '').strip() for c in COLS} for r in results]
    for r in public_rows:
        for c in COLS:
            for bad in FORBIDDEN:
                r[c] = r[c].replace(bad, '').strip()
    csv_path = out_dir / f'{file_base}.csv'
    json_path = out_dir / f'{file_base}.json'
    sqlite_path = out_dir / f'{file_base}.sqlite'
    with csv_path.open('w', newline='', encoding='utf-8-sig') as f:
        w = csv.DictWriter(f, fieldnames=COLS)
        w.writeheader(); w.writerows(public_rows)
    json_path.write_text(json.dumps(public_rows, ensure_ascii=False, indent=2), encoding='utf-8')
    con = sqlite3.connect(sqlite_path)
    con.execute('DROP TABLE IF EXISTS perfumes')
    con.execute('CREATE TABLE perfumes (brand TEXT, name TEXT, release_year TEXT, accords TEXT, top_notes TEXT, middle_notes TEXT, base_notes TEXT)')
    con.executemany('INSERT INTO perfumes VALUES (?, ?, ?, ?, ?, ?, ?)', [[r[c] for c in COLS] for r in public_rows])
    con.commit(); con.close()
    return csv_path, json_path, sqlite_path

def main():
    out_dir = Path(r'C:\Users\Playdata\Desktop\CSV\Giorgio_Armani')
    html_dir = out_dir / 'html'
    brand = 'Giorgio Armani'
    file_base = 'Giorgio_Armani'
    links = json.loads((out_dir / 'links.json').read_text(encoding='utf-8'))
    results_path = out_dir / f'{file_base}_raw.json'
    results = json.loads(results_path.read_text(encoding='utf-8')) if results_path.exists() else []
    done = {r.get('_url') for r in results}
    print(f'links={len(links)} done={len(results)} remaining={len(links)-len(results)}', flush=True)
    with sync_playwright() as p:
        browser = p.chromium.connect_over_cdp(CDP_URL)
        context = browser.contexts[0] if browser.contexts else browser.new_context()
        page = context.pages[0] if context.pages else context.new_page()
        for i, url in enumerate(links, 1):
            if url in done:
                continue
            html_path = html_dir / (url.rsplit('/', 1)[-1] + '.html')
            page.goto(url, wait_until='domcontentloaded', timeout=120000)
            time.sleep(8 + random.uniform(1, 5))
            page_html = page.content()
            if any(marker in page_html for marker in ['Just a moment', 'security verification', 'Access denied']):
                raise RuntimeError(f'anti-bot/security page still visible: {url}')
            html_path.write_text(page_html, encoding='utf-8')
            row = parse_perfume(url, page_html, brand)
            safe_name = row['name'].encode('ascii', 'backslashreplace').decode('ascii')
            print(i, safe_name, row['release_year'], 'notes', bool(row['top_notes']), bool(row['middle_notes']), bool(row['base_notes']), flush=True)
            results.append(row)
            results_path.write_text(json.dumps(results, ensure_ascii=False, indent=2), encoding='utf-8')
            time.sleep(45 + random.uniform(0, 8))
    paths = write_outputs(out_dir, file_base, results)
    print('outputs ' + ' '.join(str(p) for p in paths), flush=True)

if __name__ == '__main__':
    main()
