//! Shopping Sessions: native single-binary pipeline implementing rules.md.
//!
//! Phases
//!  1. mmap every input; read manifest, products, campaigns.
//!  2. links -> union-find -> map user-hash -> (person key, name ref).
//!  3. parse events (records partitioned by event_id hash) and clicks
//!     (valid only, partitioned by person) in parallel.
//!  4. per event_id partition: dedup (smallest ingest_ts), keep valid copies,
//!     re-partition by person.
//!  5. per person partition: sort, sessionize, credit + assist, aggregate.
//!  6. write both Parquet tables.
//!
//! Strings are never copied: a "ref" packs (file, offset, length) of the
//! unquoted bytes inside the memory-mapped input.

use std::fs::File;
use std::hash::{BuildHasherDefault, Hasher};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::OnceLock;
use std::sync::atomic::{AtomicUsize, Ordering as AO};
use std::time::Instant;

use memchr::{memchr, memchr_iter};
use memmap2::Mmap;
use xxhash_rust::xxh3::{xxh3_64, xxh3_128};

#[cfg(target_os = "linux")]
#[global_allocator]
static GLOBAL: mimalloc::MiMalloc = mimalloc::MiMalloc;

const DAY_MS: i64 = 86_400_000;
const GAP_MS: i64 = 1_800_000;
const WEEK_MS: i64 = 7 * DAY_MS;

const P1_BITS: u32 = 10; // event_id partitions
const P1: usize = 1 << P1_BITS;
const P2_BITS: u32 = 10; // person partitions
const P2: usize = 1 << P2_BITS;

const CHUNK: usize = 4 << 20;

// ------------------------------------------------------------------ files

static FILES: OnceLock<Vec<Option<Mmap>>> = OnceLock::new();

#[inline]
fn fdata(i: usize) -> &'static [u8] {
    match &FILES.get().unwrap()[i] {
        Some(m) => &m[..],
        None => &[],
    }
}

const OFF_BITS: u32 = 38;
const LEN_BITS: u32 = 14;

#[inline]
fn mkref(file: usize, off: usize, len: usize) -> u64 {
    let len = len.min((1 << LEN_BITS) - 1);
    ((file as u64) << (OFF_BITS + LEN_BITS)) | ((len as u64) << OFF_BITS) | off as u64
}

#[inline]
fn rbytes(r: u64) -> &'static [u8] {
    let file = (r >> (OFF_BITS + LEN_BITS)) as usize;
    let len = ((r >> OFF_BITS) & ((1 << LEN_BITS) - 1)) as usize;
    let off = (r & ((1 << OFF_BITS) - 1)) as usize;
    &fdata(file)[off..off + len]
}

// ------------------------------------------------------------------ hashing

#[derive(Default)]
struct IdHasher(u64);
impl Hasher for IdHasher {
    #[inline]
    fn finish(&self) -> u64 {
        self.0
    }
    #[inline]
    fn write(&mut self, _: &[u8]) {
        unreachable!()
    }
    #[inline]
    fn write_u64(&mut self, x: u64) {
        self.0 = x;
    }
    #[inline]
    fn write_u128(&mut self, x: u128) {
        self.0 = x as u64;
    }
}
type IdBuild = BuildHasherDefault<IdHasher>;
type IdMap<K, V> = hashbrown::HashMap<K, V, IdBuild>;

// ------------------------------------------------------------------ CSV

/// Split a line into exactly N fields per the dialect; returns unquoted
/// (start, end) offsets relative to the line, or false when malformed.
#[inline]
fn split<const N: usize>(line: &[u8], out: &mut [(usize, usize); N]) -> bool {
    if line.is_empty() {
        return false;
    }
    let mut start = 0usize;
    let mut k = 0usize;
    for c in memchr_iter(b',', line) {
        if k >= N - 1 {
            return false;
        }
        out[k] = (start, c);
        k += 1;
        start = c + 1;
    }
    if k != N - 1 {
        return false;
    }
    out[N - 1] = (start, line.len());
    if memchr(b'"', line).is_some() {
        for f in out.iter_mut() {
            let s = &line[f.0..f.1];
            if s.len() >= 2 && s[0] == b'"' && s[s.len() - 1] == b'"' {
                if memchr(b'"', &s[1..s.len() - 1]).is_some() {
                    return false;
                }
                f.0 += 1;
                f.1 -= 1;
            } else if memchr(b'"', s).is_some() {
                return false;
            }
        }
    }
    true
}

/// Byte ranges [start, end) of each chunk of a file's data lines.
fn chunks_of(file: usize, out: &mut Vec<(usize, usize, usize)>) {
    let d = fdata(file);
    let begin = match memchr(b'\n', d) {
        Some(i) => i + 1,
        None => return,
    };
    let mut s = begin;
    while s < d.len() {
        let mut e = (s + CHUNK).min(d.len());
        if e < d.len() {
            match memchr(b'\n', &d[e..]) {
                Some(i) => e = e + i + 1,
                None => e = d.len(),
            }
        }
        out.push((file, s, e));
        s = e;
    }
}

#[inline]
fn for_lines(d: &'static [u8], s: usize, e: usize, mut f: impl FnMut(&'static [u8], usize)) {
    let mut pos = s;
    while pos < e {
        let nl = match memchr(b'\n', &d[pos..e]) {
            Some(i) => pos + i,
            None => e,
        };
        f(&d[pos..nl], pos);
        pos = nl + 1;
    }
}

// ------------------------------------------------------------------ time

#[inline]
fn days_from_civil(y: i64, m: i64, d: i64) -> i64 {
    let y = if m <= 2 { y - 1 } else { y };
    let era = if y >= 0 { y } else { y - 399 } / 400;
    let yoe = y - era * 400;
    let mp = (m + 9) % 12;
    let doy = (153 * mp + 2) / 5 + d - 1;
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    era * 146097 + doe - 719468
}

#[inline]
fn dig2(a: u8, b: u8) -> Option<i64> {
    let x = a.wrapping_sub(b'0');
    let y = b.wrapping_sub(b'0');
    if x < 10 && y < 10 { Some((x * 10 + y) as i64) } else { None }
}

#[inline]
fn is_leap(y: i64) -> bool {
    (y % 4 == 0 && y % 100 != 0) || y % 400 == 0
}

/// F1 / F2 / F3 -> epoch ms, None when bad.
#[inline]
fn parse_ts(s: &[u8]) -> Option<i64> {
    let n = s.len();
    if n == 0 {
        return None;
    }
    if n == 20 || n == 25 {
        if s[4] == b'-' {
            return parse_iso(s);
        }
    }
    // F3: digits only
    let mut v: i64 = 0;
    let mut sig = 0usize;
    for &c in s {
        let x = c.wrapping_sub(b'0');
        if x >= 10 {
            return None;
        }
        if sig > 0 || x != 0 {
            sig += 1;
            if sig > 18 {
                // far outside any window; still a number, but bad (out of window)
                // keep scanning to confirm digits-only is irrelevant: bad either way
                return None;
            }
            v = v * 10 + x as i64;
        }
    }
    Some(v)
}

#[inline]
fn parse_iso(s: &[u8]) -> Option<i64> {
    if s[7] != b'-' || s[10] != b'T' || s[13] != b':' || s[16] != b':' {
        return None;
    }
    let y = dig2(s[0], s[1])? * 100 + dig2(s[2], s[3])?;
    let mo = dig2(s[5], s[6])?;
    let d = dig2(s[8], s[9])?;
    let h = dig2(s[11], s[12])?;
    let mi = dig2(s[14], s[15])?;
    let se = dig2(s[17], s[18])?;
    if !(1..=12).contains(&mo) || d < 1 || h > 23 || mi > 59 || se > 59 {
        return None;
    }
    let dim = match mo {
        2 => {
            if is_leap(y) {
                29
            } else {
                28
            }
        }
        4 | 6 | 9 | 11 => 30,
        _ => 31,
    };
    if d > dim {
        return None;
    }
    let mut t = ((days_from_civil(y, mo, d) * 24 + h) * 60 + mi) * 60 + se;
    if s.len() == 20 {
        if s[19] != b'Z' {
            return None;
        }
    } else {
        let sign = match s[19] {
            b'+' => 1,
            b'-' => -1,
            _ => return None,
        };
        if s[22] != b':' {
            return None;
        }
        let oh = dig2(s[20], s[21])?;
        let om = dig2(s[23], s[24])?;
        if oh > 23 || om > 59 {
            return None;
        }
        t -= sign * (oh * 3600 + om * 60);
    }
    Some(t * 1000)
}

// ------------------------------------------------------------------ records

/// Event copy as parsed, before dedup.
#[derive(Clone, Copy)]
struct ERaw {
    eid: u128,
    person: u64,
    nref: u64,
    ts: i64,
    ingest: i64,
    price: u32,
    cat: u16,
    flags: u8, // 1 = valid (incl. not late), 2 = purchase
}

/// Surviving event.
#[derive(Clone, Copy)]
struct Ev {
    person: u64,
    ts: i64,
    nref: u64,
    price: u32,
    cat: u16,
    purchase: bool,
}

#[derive(Clone, Copy)]
struct Click {
    person: u64,
    ts: i64,
    cref: u64,
    camp: u32,
}

struct URow {
    nref: u64,
    day: i32,
    sessions: u32,
    events: u32,
    revenue: u64,
    top: u16, // u16::MAX = null
}

// ------------------------------------------------------------------ context

struct Ctx {
    ws: i64,
    we: i64,
    ws_day: i64,
    ndays: usize,
    products: hashbrown::HashMap<&'static [u8], u16>,
    unknown_cat: u16,
    cats: Vec<Vec<u8>>,
    campaigns: hashbrown::HashMap<&'static [u8], u32>,
    camp_ids: Vec<Vec<u8>>,
    camp_ch: Vec<Vec<u8>>,
    links: IdMap<u64, (u64, u64)>,
}

fn list_csv(dir: &Path) -> Vec<PathBuf> {
    let mut v: Vec<PathBuf> = match std::fs::read_dir(dir) {
        Ok(rd) => rd
            .filter_map(|e| e.ok())
            .map(|e| e.path())
            .filter(|p| p.extension().map(|x| x == "csv").unwrap_or(false) && p.is_file())
            .collect(),
        Err(_) => Vec::new(),
    };
    v.sort();
    v
}

fn map_file(p: &Path) -> Option<Mmap> {
    let f = File::open(p).expect("open");
    let len = f.metadata().map(|m| m.len()).unwrap_or(0);
    if len == 0 {
        return None;
    }
    let m = unsafe { Mmap::map(&f).expect("mmap") };
    #[cfg(unix)]
    {
        let _ = m.advise(memmap2::Advice::Sequential);
    }
    Some(m)
}

fn manifest_value(text: &str, key: &str) -> i64 {
    let k = format!("\"{}\"", key);
    let i = text.find(&k).expect("manifest key") + k.len();
    let rest = &text[i..];
    let a = rest.find('"').unwrap() + 1;
    let b = a + rest[a..].find('"').unwrap();
    parse_ts(rest[a..b].as_bytes()).expect("manifest ts")
}

fn run_threads<F: Fn(usize) + Sync>(n: usize, f: F) {
    std::thread::scope(|s| {
        for t in 0..n {
            let f = &f;
            s.spawn(move || f(t));
        }
    });
}

/// Hand out work items 0..n to threads.
struct Queue(AtomicUsize);
impl Queue {
    fn new() -> Self {
        Queue(AtomicUsize::new(0))
    }
    #[inline]
    fn next(&self) -> usize {
        self.0.fetch_add(1, AO::Relaxed)
    }
}

/// Per-thread outputs handed back through a mutex-free slot vector.
struct Slots<T>(Vec<std::sync::Mutex<Option<T>>>);
impl<T> Slots<T> {
    fn new(n: usize) -> Self {
        Slots((0..n).map(|_| std::sync::Mutex::new(None)).collect())
    }
    fn put(&self, i: usize, v: T) {
        *self.0[i].lock().unwrap() = Some(v);
    }
    fn take(self) -> Vec<T> {
        self.0.into_iter().map(|m| m.into_inner().unwrap().unwrap()).collect()
    }
}

fn main() {
    let t0 = Instant::now();
    let data_dir = PathBuf::from(std::env::var("DATA_DIR").unwrap_or_else(|_| "/data".into()));
    let out_dir = PathBuf::from(std::env::var("OUTPUT_DIR").unwrap_or_else(|_| "/output".into()));
    let nt: usize = std::env::var("NCPU")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or_else(|| std::thread::available_parallelism().map(|n| n.get()).unwrap_or(8))
        .max(1);

    // ---- files
    let mut paths: Vec<PathBuf> = vec![data_dir.join("products.csv"), data_dir.join("campaigns.csv")];
    let ev_paths = list_csv(&data_dir.join("events"));
    let cl_paths = list_csv(&data_dir.join("clicks"));
    let ln_paths = list_csv(&data_dir.join("links"));
    let ev_base = paths.len();
    paths.extend(ev_paths.iter().cloned());
    let cl_base = paths.len();
    paths.extend(cl_paths.iter().cloned());
    let ln_base = paths.len();
    paths.extend(ln_paths.iter().cloned());
    let n_files = paths.len();
    assert!(n_files < (1 << (64 - OFF_BITS - LEN_BITS)), "too many files");
    let maps: Vec<Option<Mmap>> = {
        let slots: Slots<Option<Mmap>> = Slots::new(n_files);
        let q = Queue::new();
        run_threads(nt.min(n_files), |_| loop {
            let i = q.next();
            if i >= n_files {
                break;
            }
            slots.put(i, map_file(&paths[i]));
        });
        slots.take()
    };
    let _ = FILES.set(maps);

    let manifest = std::fs::read_to_string(data_dir.join("manifest.json")).expect("manifest");
    let ws = manifest_value(&manifest, "window_start");
    let we = manifest_value(&manifest, "window_end");
    let ws_day = ws.div_euclid(DAY_MS);
    let ndays = ((we - 1).div_euclid(DAY_MS) - ws_day + 1).max(1) as usize;

    // ---- products / categories
    let mut prod_raw: Vec<(&'static [u8], &'static [u8])> = Vec::new();
    {
        let d = fdata(0);
        let mut ch = Vec::new();
        chunks_of(0, &mut ch);
        for &(_, s, e) in &ch {
            for_lines(d, s, e, |line, _| {
                let mut f = [(0, 0); 2];
                if split(line, &mut f) {
                    prod_raw.push((&line[f[0].0..f[0].1], &line[f[1].0..f[1].1]));
                }
            });
        }
    }
    let mut cats: Vec<Vec<u8>> = prod_raw.iter().map(|p| p.1.to_vec()).collect();
    cats.push(b"unknown".to_vec());
    cats.sort();
    cats.dedup();
    let unknown_cat = cats.binary_search(&b"unknown".to_vec()).unwrap() as u16;
    let mut products = hashbrown::HashMap::with_capacity(prod_raw.len());
    for (p, c) in &prod_raw {
        products.insert(*p, cats.binary_search(&c.to_vec()).unwrap() as u16);
    }

    // ---- campaigns
    let mut campaigns = hashbrown::HashMap::new();
    let mut camp_ids = Vec::new();
    let mut camp_ch = Vec::new();
    {
        let d = fdata(1);
        let mut ch = Vec::new();
        chunks_of(1, &mut ch);
        for &(_, s, e) in &ch {
            for_lines(d, s, e, |line, _| {
                let mut f = [(0, 0); 2];
                if split(line, &mut f) {
                    let id = &line[f[0].0..f[0].1];
                    if !campaigns.contains_key(id) {
                        campaigns.insert(id, camp_ids.len() as u32);
                        camp_ids.push(id.to_vec());
                        camp_ch.push(line[f[1].0..f[1].1].to_vec());
                    }
                }
            });
        }
    }

    // ---- links
    let links = build_links(ln_base, ln_paths.len(), nt);
    let t_links = t0.elapsed();

    let ctx = Arc::new(Ctx {
        ws,
        we,
        ws_day,
        ndays,
        products,
        unknown_cat,
        cats,
        campaigns,
        camp_ids,
        camp_ch,
        links,
    });

    // ---- parse events + clicks
    let mut work: Vec<(usize, usize, usize)> = Vec::new();
    for i in 0..ev_paths.len() {
        chunks_of(ev_base + i, &mut work);
    }
    let n_ev_chunks = work.len();
    for i in 0..cl_paths.len() {
        chunks_of(cl_base + i, &mut work);
    }
    let ev_bytes: usize = (0..ev_paths.len()).map(|i| fdata(ev_base + i).len()).sum();
    let est_per_bucket = ev_bytes / 90 / nt / P1 + 16;

    type Parsed = (Vec<Vec<ERaw>>, Vec<Vec<Click>>);
    let parsed: Vec<Parsed> = {
        let slots: Slots<Parsed> = Slots::new(nt);
        let q = Queue::new();
        let ctx = &ctx;
        let work = &work;
        run_threads(nt, |t| {
            let mut eb: Vec<Vec<ERaw>> = (0..P1).map(|_| Vec::with_capacity(est_per_bucket)).collect();
            let mut cb: Vec<Vec<Click>> = (0..P2).map(|_| Vec::new()).collect();
            loop {
                let i = q.next();
                if i >= work.len() {
                    break;
                }
                let (file, s, e) = work[i];
                if i < n_ev_chunks {
                    parse_events(ctx, file, s, e, &mut eb);
                } else {
                    parse_clicks(ctx, file, s, e, &mut cb);
                }
            }
            slots.put(t, (eb, cb));
        });
        slots.take()
    };
    let t_parse = t0.elapsed();

    // ---- dedup per event_id partition
    let deduped: Vec<Vec<Vec<Ev>>> = {
        let slots: Slots<Vec<Vec<Ev>>> = Slots::new(nt);
        let q = Queue::new();
        let parsed = &parsed;
        run_threads(nt, |t| {
            let mut out: Vec<Vec<Ev>> = (0..P2).map(|_| Vec::new()).collect();
            let mut map: IdMap<u128, (i64, u32, u32)> = IdMap::default();
            loop {
                let p = q.next();
                if p >= P1 {
                    break;
                }
                map.clear();
                for (ti, (eb, _)) in parsed.iter().enumerate() {
                    for (i, r) in eb[p].iter().enumerate() {
                        match map.entry(r.eid) {
                            hashbrown::hash_map::Entry::Vacant(v) => {
                                v.insert((r.ingest, ti as u32, i as u32));
                            }
                            hashbrown::hash_map::Entry::Occupied(mut o) => {
                                if r.ingest < o.get().0 {
                                    o.insert((r.ingest, ti as u32, i as u32));
                                }
                            }
                        }
                    }
                }
                for &(_, ti, i) in map.values() {
                    let r = &parsed[ti as usize].0[p][i as usize];
                    if r.flags & 1 != 0 {
                        out[(r.person >> (64 - P2_BITS)) as usize].push(Ev {
                            person: r.person,
                            ts: r.ts,
                            nref: r.nref,
                            price: r.price,
                            cat: r.cat,
                            purchase: r.flags & 2 != 0,
                        });
                    }
                }
            }
            slots.put(t, out);
        });
        slots.take()
    };
    let t_dedup = t0.elapsed();

    // ---- sessionize + attribute per person partition
    let nc = ctx.camp_ids.len();
    let ncamp = nc + 1; // + organic
    type Agg = (Vec<URow>, Vec<[u64; 4]>);
    let aggs: Vec<Agg> = {
        let slots: Slots<Agg> = Slots::new(nt);
        let q = Queue::new();
        let ctx = &ctx;
        let deduped = &deduped;
        let parsed = &parsed;
        run_threads(nt, |t| {
            let mut rows: Vec<URow> = Vec::new();
            let mut camp = vec![[0u64; 4]; ncamp * ctx.ndays];
            let mut st = PersonState::new(ctx.cats.len(), nc);
            let mut evs: Vec<Ev> = Vec::new();
            let mut cls: Vec<Click> = Vec::new();
            loop {
                let p = q.next();
                if p >= P2 {
                    break;
                }
                evs.clear();
                for d in deduped.iter() {
                    evs.extend_from_slice(&d[p]);
                }
                if evs.is_empty() {
                    continue;
                }
                cls.clear();
                for (_, cb) in parsed.iter() {
                    cls.extend_from_slice(&cb[p]);
                }
                evs.sort_unstable_by_key(|e| (e.person, e.ts));
                cls.sort_unstable_by(|a, b| {
                    (a.person, a.ts)
                        .cmp(&(b.person, b.ts))
                        .then_with(|| rbytes(a.cref).cmp(rbytes(b.cref)))
                });
                let mut ci = 0usize;
                let mut i = 0usize;
                while i < evs.len() {
                    let person = evs[i].person;
                    let mut j = i + 1;
                    while j < evs.len() && evs[j].person == person {
                        j += 1;
                    }
                    while ci < cls.len() && cls[ci].person < person {
                        ci += 1;
                    }
                    let mut cj = ci;
                    while cj < cls.len() && cls[cj].person == person {
                        cj += 1;
                    }
                    st.person(ctx, &evs[i..j], &cls[ci..cj], &mut rows, &mut camp);
                    ci = cj;
                    i = j;
                }
            }
            slots.put(t, (rows, camp));
        });
        slots.take()
    };
    drop(parsed);
    drop(deduped);
    let t_sess = t0.elapsed();

    // ---- merge campaign aggregates
    let mut camp = vec![[0u64; 4]; ncamp * ndays];
    let mut user_rows: Vec<Vec<URow>> = Vec::new();
    for (rows, c) in aggs {
        for (a, b) in camp.iter_mut().zip(c.iter()) {
            for k in 0..4 {
                a[k] += b[k];
            }
        }
        user_rows.push(rows);
    }

    write_outputs(&ctx, &out_dir, user_rows, &camp, nt);
    let t_end = t0.elapsed();
    eprintln!(
        "links {:.3}s parse {:.3}s dedup {:.3}s sess {:.3}s write {:.3}s",
        t_links.as_secs_f64(),
        (t_parse - t_links).as_secs_f64(),
        (t_dedup - t_parse).as_secs_f64(),
        (t_sess - t_dedup).as_secs_f64(),
        (t_end - t_sess).as_secs_f64()
    );
    // skip destructors of large structures
    std::process::exit(0);
}

// ------------------------------------------------------------------ links

fn build_links(base: usize, n: usize, nt: usize) -> IdMap<u64, (u64, u64)> {
    let mut work = Vec::new();
    for i in 0..n {
        chunks_of(base + i, &mut work);
    }
    let parts: Vec<Vec<(u64, u64, u64, u64)>> = {
        let slots: Slots<Vec<(u64, u64, u64, u64)>> = Slots::new(nt);
        let q = Queue::new();
        let work = &work;
        run_threads(nt, |t| {
            let mut v = Vec::new();
            loop {
                let i = q.next();
                if i >= work.len() {
                    break;
                }
                let (file, s, e) = work[i];
                let d = fdata(file);
                for_lines(d, s, e, |line, pos| {
                    let mut f = [(0, 0); 2];
                    if !split(line, &mut f) {
                        return;
                    }
                    let a = &line[f[0].0..f[0].1];
                    let b = &line[f[1].0..f[1].1];
                    if a.is_empty() || b.is_empty() {
                        return;
                    }
                    v.push((
                        xxh3_64(a),
                        xxh3_64(b),
                        mkref(file, pos + f[0].0, a.len()),
                        mkref(file, pos + f[1].0, b.len()),
                    ));
                });
            }
            slots.put(t, v);
        });
        slots.take()
    };
    let total: usize = parts.iter().map(|v| v.len()).sum();
    let mut idx: IdMap<u64, u32> = IdMap::with_capacity_and_hasher(total, Default::default());
    let mut ids: Vec<(u64, u64)> = Vec::with_capacity(total);
    let mut parent: Vec<u32> = Vec::with_capacity(total);
    fn find(p: &mut [u32], mut x: u32) -> u32 {
        let mut r = x;
        while p[r as usize] != r {
            r = p[r as usize];
        }
        while p[x as usize] != r {
            let nx = p[x as usize];
            p[x as usize] = r;
            x = nx;
        }
        r
    }
    for v in &parts {
        for &(ha, hb, ra, rb) in v {
            let mut get = |h: u64, r: u64| -> u32 {
                *idx.entry(h).or_insert_with(|| {
                    let i = ids.len() as u32;
                    ids.push((h, r));
                    parent.push(i);
                    i
                })
            };
            let a = get(ha, ra);
            let b = get(hb, rb);
            let x = find(&mut parent, a);
            let y = find(&mut parent, b);
            if x != y {
                // attach larger index under smaller (cheap, plus path compression)
                if x < y {
                    parent[y as usize] = x;
                } else {
                    parent[x as usize] = y;
                }
            }
        }
    }
    let m = ids.len();
    let mut best: Vec<u32> = (0..m as u32).collect();
    for i in 0..m as u32 {
        let r = find(&mut parent, i) as usize;
        let b = best[r];
        if b != i && rbytes(ids[i as usize].1) < rbytes(ids[b as usize].1) {
            best[r] = i;
        }
    }
    let mut out: IdMap<u64, (u64, u64)> = IdMap::with_capacity_and_hasher(m, Default::default());
    for i in 0..m {
        let r = parent[i] as usize; // fully compressed by find above
        let b = best[find(&mut parent, r as u32) as usize] as usize;
        out.insert(ids[i].0, ids[b]);
    }
    out
}

// ------------------------------------------------------------------ parsing

#[inline]
fn parse_price(s: &[u8]) -> Option<u32> {
    if s.is_empty() {
        return None;
    }
    let mut v: u64 = 0;
    let mut sig = 0;
    for &c in s {
        let x = c.wrapping_sub(b'0');
        if x >= 10 {
            return None;
        }
        if sig > 0 || x != 0 {
            sig += 1;
            if sig > 8 {
                return None;
            }
            v = v * 10 + x as u64;
        }
    }
    if v >= 1 && v <= 10_000_000 { Some(v as u32) } else { None }
}

fn parse_events(ctx: &Ctx, file: usize, s: usize, e: usize, eb: &mut [Vec<ERaw>]) {
    let d = fdata(file);
    for_lines(d, s, e, |line, pos| {
        let mut f = [(0, 0); 7];
        if !split(line, &mut f) {
            return;
        }
        let g = |k: usize| &line[f[k].0..f[k].1];
        let eid = g(0);
        if eid.is_empty() {
            return;
        }
        let h = xxh3_128(eid);
        let ingest = parse_ts(g(3)).unwrap_or(i64::MAX);
        let mut r = ERaw { eid: h, person: 0, nref: 0, ts: 0, ingest, price: 0, cat: 0, flags: 0 };
        'v: {
            let user = g(1);
            if user.is_empty() {
                break 'v;
            }
            let ts = match parse_ts(g(2)) {
                Some(t) if t >= ctx.ws && t < ctx.we => t,
                _ => break 'v,
            };
            let ty = g(4);
            let purchase = match ty {
                b"view" | b"cart" => false,
                b"purchase" => true,
                _ => break 'v,
            };
            let prod = g(5);
            if prod.is_empty() {
                break 'v;
            }
            let mut price = 0;
            if purchase {
                price = match parse_price(g(6)) {
                    Some(p) => p,
                    None => break 'v,
                };
            }
            if ingest == i64::MAX || ingest - ts > DAY_MS {
                break 'v;
            }
            let uh = xxh3_64(user);
            let (person, nref) = match ctx.links.get(&uh) {
                Some(&(pk, nr)) => (pk, nr),
                None => (uh, mkref(file, pos + f[1].0, user.len())),
            };
            r.person = person;
            r.nref = nref;
            r.ts = ts;
            r.price = price;
            r.cat = *ctx.products.get(prod).unwrap_or(&ctx.unknown_cat);
            r.flags = 1 | if purchase { 2 } else { 0 };
        }
        eb[(h >> (128 - P1_BITS)) as usize].push(r);
    });
}

fn parse_clicks(ctx: &Ctx, file: usize, s: usize, e: usize, cb: &mut [Vec<Click>]) {
    let d = fdata(file);
    let lo = ctx.ws - WEEK_MS;
    for_lines(d, s, e, |line, pos| {
        let mut f = [(0, 0); 4];
        if !split(line, &mut f) {
            return;
        }
        let g = |k: usize| &line[f[k].0..f[k].1];
        let cid = g(0);
        if cid.is_empty() {
            return;
        }
        let user = g(1);
        if user.is_empty() {
            return;
        }
        let ts = match parse_ts(g(3)) {
            Some(t) if t >= lo && t < ctx.we => t,
            _ => return,
        };
        let camp = match ctx.campaigns.get(g(2)) {
            Some(&c) => c,
            None => return,
        };
        let uh = xxh3_64(user);
        let person = match ctx.links.get(&uh) {
            Some(&(pk, _)) => pk,
            None => uh,
        };
        cb[(person >> (64 - P2_BITS)) as usize].push(Click {
            person,
            ts,
            cref: mkref(file, pos + f[0].0, cid.len()),
            camp,
        });
    });
}

// ------------------------------------------------------------------ per person

struct Sess {
    start: i64,
    day: u32, // index from window start day
    revenue: u64,
    purchase: bool,
}

struct PersonState {
    catrev: Vec<u64>,
    sess: Vec<Sess>,
    run_end: Vec<u32>,
    cnt: Vec<u32>,
    act: Vec<u32>,
}

impl PersonState {
    fn new(ncat: usize, nc: usize) -> Self {
        PersonState {
            catrev: vec![0; ncat],
            sess: Vec::new(),
            run_end: Vec::new(),
            cnt: vec![0; nc],
            act: vec![0; nc],
        }
    }

    fn person(&mut self, ctx: &Ctx, evs: &[Ev], cls: &[Click], rows: &mut Vec<URow>, camp: &mut [[u64; 4]]) {
        let nd = ctx.ndays;
        let nref = evs[0].nref;
        self.sess.clear();
        // sessionize + user_daily
        let mut cur_day: i64 = i64::MIN;
        let mut row = URow { nref, day: 0, sessions: 0, events: 0, revenue: 0, top: u16::MAX };
        let mut any_purchase_row = false;
        let mut prev = i64::MIN;
        for e in evs {
            if prev == i64::MIN || e.ts - prev > GAP_MS {
                let day = e.ts.div_euclid(DAY_MS);
                if day != cur_day {
                    if cur_day != i64::MIN {
                        self.flush_row(&mut row, any_purchase_row, rows);
                    }
                    cur_day = day;
                    row = URow { nref, day: day as i32, sessions: 0, events: 0, revenue: 0, top: u16::MAX };
                    any_purchase_row = false;
                }
                row.sessions += 1;
                self.sess.push(Sess { start: e.ts, day: (day - ctx.ws_day) as u32, revenue: 0, purchase: false });
            }
            prev = e.ts;
            row.events += 1;
            if e.purchase {
                let p = e.price as u64;
                row.revenue += p;
                self.catrev[e.cat as usize] += p;
                any_purchase_row = true;
                let s = self.sess.last_mut().unwrap();
                s.revenue += p;
                s.purchase = true;
            }
        }
        self.flush_row(&mut row, any_purchase_row, rows);

        let n = self.sess.len();
        let organic = ctx.camp_ids.len();
        if cls.is_empty() {
            for s in &self.sess {
                let c = &mut camp[organic * nd + s.day as usize];
                c[0] += 1;
                c[1] += s.purchase as u64;
                c[2] += s.revenue;
            }
            return;
        }
        // day runs for assist emission
        self.run_end.clear();
        self.run_end.resize(n, 0);
        let mut k = n;
        while k > 0 {
            let i = k - 1;
            self.run_end[i] = if i + 1 < n && self.sess[i + 1].day == self.sess[i].day {
                self.run_end[i + 1]
            } else {
                (i + 1) as u32
            };
            k -= 1;
        }
        let mut lo = 0usize;
        let mut hi = 0usize;
        for i in 0..n {
            let s = self.sess[i].start;
            while hi < cls.len() && cls[hi].ts <= s {
                let c = cls[hi].camp as usize;
                if self.cnt[c] == 0 {
                    self.act[c] = i as u32;
                }
                self.cnt[c] += 1;
                hi += 1;
            }
            while lo < hi && cls[lo].ts < s - WEEK_MS {
                let c = cls[lo].camp as usize;
                self.cnt[c] -= 1;
                if self.cnt[c] == 0 {
                    self.emit(nd, c, self.act[c] as usize, i, camp);
                }
                lo += 1;
            }
            let credited = if hi > lo {
                let mut k = hi - 1;
                while k > lo && cls[k - 1].ts == cls[k].ts {
                    k -= 1;
                }
                cls[k].camp as usize
            } else {
                organic
            };
            let ss = &self.sess[i];
            let c = &mut camp[credited * nd + ss.day as usize];
            c[0] += 1;
            c[1] += ss.purchase as u64;
            c[2] += ss.revenue;
        }
        for k in lo..hi {
            let c = cls[k].camp as usize;
            if self.cnt[c] > 0 {
                self.cnt[c] = 0;
                self.emit(nd, c, self.act[c] as usize, n, camp);
            }
        }
    }

    #[inline]
    fn emit(&self, nd: usize, c: usize, a: usize, b: usize, camp: &mut [[u64; 4]]) {
        let mut i = a;
        while i < b {
            let e = (self.run_end[i] as usize).min(b);
            camp[c * nd + self.sess[i].day as usize][3] += (e - i) as u64;
            i = e;
        }
    }

    #[inline]
    fn flush_row(&mut self, row: &mut URow, any: bool, rows: &mut Vec<URow>) {
        if any {
            let mut best = 0usize;
            for k in 1..self.catrev.len() {
                if self.catrev[k] > self.catrev[best] {
                    best = k;
                }
            }
            row.top = best as u16;
            self.catrev.iter_mut().for_each(|x| *x = 0);
        }
        rows.push(URow { ..*row });
    }
}

// ------------------------------------------------------------------ output

fn write_outputs(ctx: &Ctx, out: &Path, user_rows: Vec<Vec<URow>>, camp: &[[u64; 4]], nt: usize) {
    use arrow_array::{ArrayRef, Date32Array, Int64Array, RecordBatch, StringArray};
    use arrow_schema::{DataType, Field, Schema};
    use parquet::arrow::ArrowWriter;
    use parquet::file::properties::{EnabledStatistics, WriterProperties};

    let ud = out.join("user_daily");
    let cd = out.join("campaign_daily");
    std::fs::create_dir_all(&ud).unwrap();
    std::fs::create_dir_all(&cd).unwrap();

    let props = || {
        WriterProperties::builder()
            .set_dictionary_enabled(false)
            .set_statistics_enabled(EnabledStatistics::None)
            .set_max_row_group_row_count(Some(1 << 20))
            .build()
    };

    let user_schema = Arc::new(Schema::new(vec![
        Field::new("user_id", DataType::Utf8, false),
        Field::new("day", DataType::Date32, false),
        Field::new("sessions", DataType::Int64, false),
        Field::new("events", DataType::Int64, false),
        Field::new("revenue_cents", DataType::Int64, false),
        Field::new("top_category", DataType::Utf8, true),
    ]));

    let cats: Vec<String> = ctx.cats.iter().map(|c| String::from_utf8_lossy(c).into_owned()).collect();
    let q = Queue::new();
    let ur = &user_rows;
    let cats = &cats;
    let user_schema = &user_schema;
    run_threads(nt.min(user_rows.len()).max(1), |_| loop {
        let i = q.next();
        if i >= ur.len() {
            break;
        }
        let rows = &ur[i];
        let ids: StringArray = rows.iter().map(|r| Some(String::from_utf8_lossy(rbytes(r.nref)))).collect();
        let days = Date32Array::from_iter_values(rows.iter().map(|r| r.day));
        let s = Int64Array::from_iter_values(rows.iter().map(|r| r.sessions as i64));
        let ev = Int64Array::from_iter_values(rows.iter().map(|r| r.events as i64));
        let rev = Int64Array::from_iter_values(rows.iter().map(|r| r.revenue as i64));
        let top: StringArray = rows
            .iter()
            .map(|r| if r.top == u16::MAX { None } else { Some(cats[r.top as usize].as_str()) })
            .collect();
        let batch = RecordBatch::try_new(
            user_schema.clone(),
            vec![
                Arc::new(ids) as ArrayRef,
                Arc::new(days),
                Arc::new(s),
                Arc::new(ev),
                Arc::new(rev),
                Arc::new(top),
            ],
        )
        .unwrap();
        let f = File::create(ud.join(format!("part-{:05}.parquet", i))).unwrap();
        let mut w = ArrowWriter::try_new(std::io::BufWriter::with_capacity(1 << 20, f), user_schema.clone(), Some(props())).unwrap();
        w.write(&batch).unwrap();
        w.close().unwrap();
    });

    // campaign_daily
    let nd = ctx.ndays;
    let nc = ctx.camp_ids.len();
    let mut cid = Vec::new();
    let mut ch = Vec::new();
    let mut day = Vec::new();
    let mut cols = [Vec::new(), Vec::new(), Vec::new(), Vec::new()];
    for c in 0..=nc {
        for d in 0..nd {
            let a = camp[c * nd + d];
            if a[0] == 0 && a[3] == 0 {
                continue;
            }
            if c == nc {
                cid.push("organic".to_string());
                ch.push("organic".to_string());
            } else {
                cid.push(String::from_utf8_lossy(&ctx.camp_ids[c]).into_owned());
                ch.push(String::from_utf8_lossy(&ctx.camp_ch[c]).into_owned());
            }
            day.push((ctx.ws_day + d as i64) as i32);
            let assisted = if c == nc { 0 } else { a[3] };
            cols[0].push(a[0] as i64);
            cols[1].push(a[1] as i64);
            cols[2].push(a[2] as i64);
            cols[3].push(assisted as i64);
        }
    }
    let schema = Arc::new(Schema::new(vec![
        Field::new("campaign_id", DataType::Utf8, false),
        Field::new("channel", DataType::Utf8, false),
        Field::new("day", DataType::Date32, false),
        Field::new("sessions", DataType::Int64, false),
        Field::new("purchasing_sessions", DataType::Int64, false),
        Field::new("revenue_cents", DataType::Int64, false),
        Field::new("assisted_sessions", DataType::Int64, false),
    ]));
    let [c0, c1, c2, c3] = cols;
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(StringArray::from(cid)) as ArrayRef,
            Arc::new(StringArray::from(ch)),
            Arc::new(Date32Array::from(day)),
            Arc::new(Int64Array::from(c0)),
            Arc::new(Int64Array::from(c1)),
            Arc::new(Int64Array::from(c2)),
            Arc::new(Int64Array::from(c3)),
        ],
    )
    .unwrap();
    let f = File::create(cd.join("part-00000.parquet")).unwrap();
    let mut w = ArrowWriter::try_new(f, schema, Some(props())).unwrap();
    w.write(&batch).unwrap();
    w.close().unwrap();
}
